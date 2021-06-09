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

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.logging.LogRedaction;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.connect.kafka.config.sink.CouchbaseSinkConfig;
import com.couchbase.connect.kafka.sink.*;
import com.couchbase.connect.kafka.util.DocumentIdExtractor;
import com.couchbase.connect.kafka.util.DocumentPathExtractor;
import com.couchbase.connect.kafka.util.DurabilitySetter;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import com.couchbase.connect.kafka.util.ScopeAndCollection;
import com.couchbase.connect.kafka.util.TopicMap;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static com.couchbase.client.core.util.CbStrings.removeStart;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CouchbaseSinkTask extends SinkTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSinkTask.class);

  private String bucketName;
  private ScopeAndCollection defaultDestCollection;
  private Map<String, ScopeAndCollection> topicToCollection;
  private KafkaCouchbaseClient client;
  private JsonConverter converter;
  private DocumentIdExtractor documentIdExtractor;
  private DocumentMode documentMode;

  private SubDocumentWriter subDocumentWriter;
  private TargetedDocWriter targetedDocWriter;
  private N1qlWriter n1qlWriter;

  private DurabilitySetter durabilitySetter;

  private Duration documentExpiry;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    CouchbaseSinkConfig config;
    try {
      config = ConfigHelper.parse(CouchbaseSinkConfig.class, properties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start CouchbaseSinkTask due to configuration error", e);
    }

    Map<String, String> clusterEnvProperties = new HashMap<>();
    properties.forEach((key, value) -> {
      if (key.startsWith("couchbase.env.") && !isNullOrEmpty(value)) {
        clusterEnvProperties.put(removeStart(key, "couchbase.env."), value);
      }
    });

    LOGGER.info("Custom ClusterEnvironment properties: {}", clusterEnvProperties);

    LogRedaction.setRedactionLevel(config.logRedaction());
    client = new KafkaCouchbaseClient(config, clusterEnvProperties);
    bucketName = config.bucket();
    defaultDestCollection = ScopeAndCollection.parse(config.defaultCollection());
    topicToCollection = TopicMap.parse(config.topicToCollection());

    converter = new JsonConverter();
    converter.configure(mapOf("schemas.enable", false), false);

    String docIdPointer = config.documentId();
    if (docIdPointer != null && !docIdPointer.isEmpty()) {
      documentIdExtractor = new DocumentIdExtractor(docIdPointer, config.removeDocumentId());
    }

    documentMode = config.documentMode();
    durabilitySetter = DurabilitySetter.create(config);
    documentExpiry = config.documentExpiration();

    switch (documentMode) {
      case TARGETED: {
        String path = config.subdocumentPath();
        targetedDocWriter = new TargetedDocWriter(path);
        break;
      }
      case SUBDOCUMENT: {
        SubDocumentMode subDocumentMode = config.subdocumentOperation();
        String path = config.subdocumentPath();
        boolean createPaths = config.subdocumentCreatePath();
        boolean createDocuments = config.createDocument();

        subDocumentWriter = new SubDocumentWriter(subDocumentMode, path, createPaths, createDocuments, documentExpiry);
        break;
      }
      case N1QL: {
        N1qlMode n1qlMode = config.n1qlOperation();
        boolean createDocuments = config.createDocument();
        List<String> n1qlWhereFields = config.n1qlWhereFields();

        n1qlWriter = new N1qlWriter(n1qlMode, n1qlWhereFields, createDocuments);
        break;
      }
    }
  }

  private static class SinkRecordAndDocument {
    private final SinkRecord sinkRecord;
    private final JsonBinaryDocument document;

    public SinkRecordAndDocument(SinkRecord sinkRecord, JsonBinaryDocument document) {
      this.sinkRecord = requireNonNull(sinkRecord);
      this.document = document; // nullable
    }
  }

  @Override
  public void put(java.util.Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    LOGGER.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the Couchbase...",
        recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

    Map<String, SinkRecordAndDocument> idToDocumentOrNull = toJsonBinaryDocuments(records);

    Flux.fromIterable(idToDocumentOrNull.entrySet())
        .flatMap(entry -> {
          SinkRecordAndDocument sinkRecordAndDocument = entry.getValue();
          SinkRecord sinkRecord = sinkRecordAndDocument.sinkRecord;
          JsonBinaryDocument doc = sinkRecordAndDocument.document;

          ScopeAndCollection destCollectionSpec = topicToCollection.getOrDefault(sinkRecord.topic(), defaultDestCollection);
          Collection destCollection = client.collection(destCollectionSpec);

          if (doc == null) {
            return removeIfExists(destCollection, entry.getKey());
          }

          switch (documentMode) {
            case TARGETED: {
              return targetedDocWriter.write(destCollection.reactive(), doc, durabilitySetter);
            }
            case N1QL: {
              return n1qlWriter.write(client.cluster(), bucketName, doc);
            }
            case SUBDOCUMENT: {
              return subDocumentWriter.write(destCollection.reactive(), doc, durabilitySetter);
            }
            default: {
              UpsertOptions options = UpsertOptions.upsertOptions()
                  .expiry(documentExpiry)
                  .transcoder(RawJsonTranscoder.INSTANCE);
              durabilitySetter.accept(options);

              return destCollection.reactive()
                  .upsert(doc.id(), doc.content(), options)
                  .then();
            }
          }
        }).blockLast();
  }

  /**
   * Converts Kafka records to documents and indexes them by document ID.
   * <p>
   * If there are duplicate document IDs, ignores all but the last. This
   * prevents a stale version of the document from "winning" by being the
   * last one written to Couchbase.
   *
   * @return a map where the key is the ID of a document, and the value is the document.
   * A null value indicates the document should be deleted.
   */
  private Map<String, SinkRecordAndDocument> toJsonBinaryDocuments(java.util.Collection<SinkRecord> records) {
    Map<String, SinkRecordAndDocument> idToSourceRecordAndDocument = new HashMap<>();
    for (SinkRecord record : records) {
      if (record.value() == null) {
        String documentId = documentIdFromKafkaMetadata(record);
        idToSourceRecordAndDocument.put(documentId, new SinkRecordAndDocument(record, null));
        continue;
      }

      JsonBinaryDocument doc = convert(record);
      idToSourceRecordAndDocument.put(doc.id(), new SinkRecordAndDocument(record, doc));
    }

    int deduplicatedRecords = records.size() - idToSourceRecordAndDocument.size();
    if (deduplicatedRecords != 0) {
      LOGGER.debug("Batch contained {} redundant Kafka records.", deduplicatedRecords);
    }

    return idToSourceRecordAndDocument;
  }

  private Mono<Void> removeIfExists(Collection collection, String documentId) {
    RemoveOptions options = removeOptions();
    durabilitySetter.accept(options);
    return collection.reactive()
        .remove(documentId, options)
        .onErrorResume(DocumentNotFoundException.class, throwable -> Mono.empty())
        .then();
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
        return documentIdExtractor.extractDocumentId(valueAsJsonBytes);
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

    return new JsonBinaryDocument(defaultId, valueAsJsonBytes);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  @Override
  public void stop() {
    if (client != null) {
      client.close();
      client = null;
    }
  }
}
