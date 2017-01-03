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

import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.connect.kafka.util.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CouchbaseSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSinkTask.class);
    private Map<String, String> configProperties;
    private CouchbaseSinkTaskConfig config;
    private Bucket bucket;
    private CouchbaseCluster cluster;
    private JsonConverter converter;

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

        List<String> clusterAddress = config.getList(CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_CONFIG);
        String bucketName = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_CONFIG);
        String password = config.getPassword(CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG).value();

        boolean sslEnabled = config.getBoolean(CouchbaseSourceConnectorConfig.CONNECTION_SSL_ENABLED_CONFIG);
        String sslKeystoreLocation = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_LOCATION_CONFIG);
        String sslKeystorePassword = config.getPassword(CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG).value();
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .sslEnabled(sslEnabled)
                .sslKeystoreFile(sslKeystoreLocation)
                .sslKeystorePassword(sslKeystorePassword)
                .build();
        cluster = CouchbaseCluster.create(env, clusterAddress);
        bucket = cluster.openBucket(bucketName, password);
        converter = new JsonConverter();
        converter.configure(Collections.singletonMap("schemas.enable", false), false);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        LOGGER.info("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the Couchbase...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

        for (SinkRecord record : records) {
            bucket.upsert(convert(record));
        }
    }

    private Document convert(SinkRecord record) {
        String id;
        Object key = record.key();
        Schema keySchema = record.keySchema();
        if (key != null && keySchema.type().isPrimitive()) {
            if (record.keySchema().type() == Schema.Type.BYTES) {
                final byte[] bytes;
                if (key instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) key).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) key;
                }
                id = new String(bytes, CharsetUtil.UTF_8);
            } else {
                id = record.key().toString();
            }
        } else {
            id = String.format("%s/%d/%d", record.topic(), record.kafkaPartition(), record.kafkaOffset());
        }
        byte[] valueData = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        return RawJsonDocument.create(id, new String(valueData, CharsetUtil.UTF_8));
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void stop() {
        cluster.disconnect();
    }
}
