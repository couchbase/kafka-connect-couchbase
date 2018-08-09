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

import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.logging.RedactionLevel;
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
import com.couchbase.connect.kafka.sink.DocumentMode;
import com.couchbase.connect.kafka.sink.N1qlMode;
import com.couchbase.connect.kafka.sink.N1qlWriter;
import com.couchbase.connect.kafka.sink.SubDocumentMode;
import com.couchbase.connect.kafka.sink.SubDocumentWriter;
import com.couchbase.connect.kafka.util.*;
import com.couchbase.connect.kafka.util.config.DurationParser;
import com.couchbase.connect.kafka.util.config.Password;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;
import static com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig.DOCUMENT_ID_POINTER_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig.DOCUMENT_MODE_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig.EXPIRY_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig.N1QL_MODE_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig.PERSIST_TO_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig.REMOVE_DOCUMENT_ID_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig.REPLICATE_TO_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSinkConnectorConfig.SUBDOCUMENT_MODE_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnector.setForceIpv4;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.FORCE_IPV4_CONFIG;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CouchbaseSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSinkTask.class);

    private Map<String, String> configProperties;
    private CouchbaseSinkTaskConfig config;
    private Bucket bucket;
    private CouchbaseCluster cluster;
    private JsonConverter converter;
    private DocumentIdExtractor documentIdExtractor;
    private String path;
    private DocumentMode documentMode;

    private SubDocumentWriter subDocumentWriter;
    private SubDocumentMode subDocumentMode;

    private N1qlWriter n1qlWriter;
    private N1qlMode n1qlMode;

    private boolean createPaths;
    private boolean createDocuments;

    private PersistTo persistTo;
    private ReplicateTo replicateTo;

    private long expiryOffsetSeconds;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            configProperties = properties;
            config = new CouchbaseSinkTaskConfig(configProperties);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start CouchbaseSinkTask due to configuration error", e);
        }

        setForceIpv4(config.getBoolean(FORCE_IPV4_CONFIG));

        RedactionLevel redactionLevel = config.getEnum(RedactionLevel.class, CouchbaseSourceConnectorConfig.LOG_REDACTION_CONFIG);
        CouchbaseLoggerFactory.setRedactionLevel(redactionLevel);

        List<String> clusterAddress = config.getList(CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_CONFIG);
        String bucketName = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_CONFIG);
        String username = config.getUsername();
        String password = Password.CONNECTION.get(config);

        boolean sslEnabled = config.getBoolean(CouchbaseSourceConnectorConfig.CONNECTION_SSL_ENABLED_CONFIG);
        String sslKeystoreLocation = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_LOCATION_CONFIG);
        String sslKeystorePassword = Password.SSL_KEYSTORE.get(config);
        Long connectTimeout = config.getLong(CouchbaseSourceConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG);
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .sslEnabled(sslEnabled)
                .sslKeystoreFile(sslKeystoreLocation)
                .sslKeystorePassword(sslKeystorePassword)
                .connectTimeout(connectTimeout)
                .build();
        cluster = CouchbaseCluster.create(env, clusterAddress);
        cluster.authenticate(username, password);

        List<Transcoder<? extends Document, ?>> transcoders =
                Collections.<Transcoder<? extends Document, ?>>singletonList(new JsonBinaryTranscoder());
        bucket = cluster.openBucket(bucketName, transcoders);

        converter = new JsonConverter();
        converter.configure(Collections.singletonMap("schemas.enable", false), false);

        String docIdPointer = config.getString(DOCUMENT_ID_POINTER_CONFIG);
        if (docIdPointer != null && !docIdPointer.isEmpty()) {
            documentIdExtractor = new DocumentIdExtractor(docIdPointer, config.getBoolean(REMOVE_DOCUMENT_ID_CONFIG));
        }

        documentMode = config.getEnum(DocumentMode.class, DOCUMENT_MODE_CONFIG);
        persistTo = config.getEnum(PersistTo.class, PERSIST_TO_CONFIG);
        replicateTo = config.getEnum(ReplicateTo.class, REPLICATE_TO_CONFIG);

        final String expiryDuration = config.getString(EXPIRY_CONFIG);
        expiryOffsetSeconds = expiryDuration.isEmpty() ? 0 : DurationParser.parseDuration(expiryDuration, SECONDS);

        switch (documentMode) {
            case SUBDOCUMENT: {
                subDocumentMode = config.getEnum(SubDocumentMode.class, SUBDOCUMENT_MODE_CONFIG);
                path = config.getString(CouchbaseSinkConnectorConfig.SUBDOCUMENT_PATH_CONFIG);
                createPaths = config.getBoolean(CouchbaseSinkConnectorConfig.SUBDOCUMENT_CREATEPATH_CONFIG);
                createDocuments = config.getBoolean(CouchbaseSinkConnectorConfig.SUBDOCUMENT_CREATEDOCUMENT_CONFIG);

                subDocumentWriter = new SubDocumentWriter(subDocumentMode, path, path.startsWith("/"), createPaths, createDocuments);
                break;
            }
            case N1QL: {
                n1qlMode = config.getEnum(N1qlMode.class, N1QL_MODE_CONFIG);
                createDocuments = config.getBoolean(CouchbaseSinkConnectorConfig.SUBDOCUMENT_CREATEDOCUMENT_CONFIG);
                n1qlWriter = new N1qlWriter(n1qlMode, createDocuments);
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
                                ? Observable.<JsonDocument>empty()
                                : Observable.<JsonDocument>error(throwable);
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
