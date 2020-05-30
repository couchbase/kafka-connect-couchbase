/*
 * Copyright (c) 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka;

import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.transcoder.Transcoder;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.couchbase.connect.kafka.config.sink.CouchbaseSinkConfig;
import com.couchbase.connect.kafka.sink.DocumentMode;
import com.couchbase.connect.kafka.sink.N1qlMode;
import com.couchbase.connect.kafka.sink.N1qlWriter;
import com.couchbase.connect.kafka.sink.SubDocumentMode;
import com.couchbase.connect.kafka.sink.SubDocumentWriter;
import com.couchbase.connect.kafka.util.DocumentIdExtractor;
import com.couchbase.connect.kafka.util.DocumentPathExtractor;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import com.couchbase.connect.kafka.util.JsonBinaryTranscoder;
import com.couchbase.connect.kafka.util.Version;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;
import static com.couchbase.connect.kafka.CouchbaseSourceConnector.setForceIpv4;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CouchbaseSinkTask extends SinkTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSinkTask.class);

  private Bucket bucket;
  private CouchbaseCluster cluster;
  private JsonConverter converter;
  private DocumentIdExtractor documentIdExtractor;
  private DocumentMode documentMode;

  private SubDocumentWriter subDocumentWriter;
  private N1qlWriter n1qlWriter;

  private PersistTo persistTo;
  private ReplicateTo replicateTo;

  private long expiryOffsetSeconds;

  @Override
  public String version() {
    return Version.getVersion();
  }

  static NetworkResolution parseNetworkResolution(String s) {
    return s.isEmpty() ? NetworkResolution.AUTO : NetworkResolution.custom(s);
  }

  @Override
  public void start(Map<String, String> properties) {
    CouchbaseSinkConfig config;
    try {
      config = ConfigHelper.parse(CouchbaseSinkConfig.class, properties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start CouchbaseSinkTask due to configuration error", e);
    }

    setForceIpv4(config.forceIPv4());
    CouchbaseLoggerFactory.setRedactionLevel(config.logRedaction());

    List<String> clusterAddress = config.seedNodes();
    NetworkResolution networkResolution = parseNetworkResolution(config.network());

    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        .sslEnabled(config.enableTls())
        .networkResolution(networkResolution)
        .sslKeystoreFile(config.trustStorePath())
        .sslKeystorePassword(config.trustStorePassword().value())
        .connectTimeout(config.bootstrapTimeout().toMillis())
        .mutationTokensEnabled(true)
        .build();
    cluster = CouchbaseCluster.create(env, clusterAddress);
    cluster.authenticate(config.username(), config.password().value());

    List<Transcoder<? extends Document, ?>> transcoders =
        Collections.singletonList(new JsonBinaryTranscoder());
    bucket = cluster.openBucket(config.bucket(), transcoders);

    converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", false), false);

    String docIdPointer = config.documentId();
    if (docIdPointer != null && !docIdPointer.isEmpty()) {
      documentIdExtractor = new DocumentIdExtractor(docIdPointer, config.removeDocumentId());
    }

    documentMode = config.documentMode();
    persistTo = config.persistTo();
    replicateTo = config.replicateTo();

    final Duration expiryDuration = config.documentExpiration();
    expiryOffsetSeconds = expiryDuration.isZero() ? 0 : expiryDuration.toMillis() / 1000;

    boolean createDocuments;
    switch (documentMode) {
      case SUBDOCUMENT: {
        SubDocumentMode subDocumentMode = config.subdocumentOperation();
        String path = config.subdocumentPath();
        boolean createPaths = config.subdocumentCreatePath();
        createDocuments = config.subdocumentCreateDocument();

        subDocumentWriter = new SubDocumentWriter(subDocumentMode, path, path.startsWith("/"), createPaths, createDocuments);
        break;
      }
      case N1QL: {
        N1qlMode n1qlMode = config.n1qlOperation();
        createDocuments = config.subdocumentCreateDocument();
        List<String> n1qlWhereFields = config.n1qlWhereFields();

        n1qlWriter = new N1qlWriter(n1qlMode, n1qlWhereFields, createDocuments);
        break;
      }
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    LOGGER.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the Couchbase...",
        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

    //noinspection unchecked
    Observable.from(records)
        .flatMapCompletable(new Func1<SinkRecord, Completable>() {
          @Override
          public Completable call(SinkRecord record) {
            if (record.value() == null) {
              String documentId = documentIdFromKafkaMetadata(record);
              return removeIfExists(documentId);
            }

            JsonBinaryDocument doc = convert(record);

            switch (documentMode) {
              case N1QL: {
                return n1qlWriter.write(bucket.async(), doc, persistTo, replicateTo);
              }
              case SUBDOCUMENT: {
                return subDocumentWriter.write(bucket.async(), doc, persistTo, replicateTo);
              }
              default: {
                return bucket.async()
                    .upsert(doc, persistTo, replicateTo)
                    .toCompletable();
              }
            }
          }
        })
        .retryWhen(
            // TODO: make it configurable
            RetryBuilder
                .anyOf(RuntimeException.class)
                .delay(Delay.exponential(TimeUnit.SECONDS, 5))
                .max(5)
                .build())

        .toCompletable().await();
  }

  private Completable removeIfExists(String documentId) {
    return bucket.async().remove(documentId, persistTo, replicateTo)
        .onErrorResumeNext(new Func1<Throwable, Observable<JsonDocument>>() {
          @Override
          public Observable<JsonDocument> call(Throwable throwable) {
            return (throwable instanceof DocumentDoesNotExistException)
                ? Observable.empty()
                : Observable.error(throwable);
          }
        }).toCompletable();
  }

  private static String toString(ByteBuffer byteBuffer) {
    final ByteBuffer sliced = byteBuffer.slice();
    byte[] bytes = new byte[sliced.remaining()];
    sliced.get(bytes);
    return new String(bytes, UTF_8);
  }

  private static String documentIdFromKafkaMetadata(SinkRecord record) {
    Object key = record.key();

    if (key instanceof String
        || key instanceof Number
        || key instanceof Boolean) {
      return key.toString();
    }

    if (key instanceof byte[]) {
      return new String((byte[]) key, UTF_8);
    }

    if (key instanceof ByteBuffer) {
      return toString((ByteBuffer) key);
    }

    return record.topic() + "/" + record.kafkaPartition() + "/" + record.kafkaOffset();
  }


  private JsonBinaryDocument convert(SinkRecord record) {

    byte[] valueAsJsonBytes = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    String defaultId = null;

    try {
      if (documentIdExtractor != null) {
        return documentIdExtractor.extractDocumentId(valueAsJsonBytes, getAbsoluteExpirySeconds());
      }

    } catch (DocumentPathExtractor.DocumentPathNotFoundException e) {
      defaultId = documentIdFromKafkaMetadata(record);
      LOGGER.warn(e.getMessage() + "; using fallback ID '{}'", defaultId);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (defaultId == null) {
      defaultId = documentIdFromKafkaMetadata(record);
    }

    return JsonBinaryDocument.create(defaultId, getAbsoluteExpirySeconds(), valueAsJsonBytes);
  }

  private int getAbsoluteExpirySeconds() {
    if (expiryOffsetSeconds == 0) {
      return 0; // no expiration
    }

    return (int) (MILLISECONDS.toSeconds(System.currentTimeMillis()) + expiryOffsetSeconds);
  }


  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  @Override
  public void stop() {
    cluster.disconnect();
  }
}
