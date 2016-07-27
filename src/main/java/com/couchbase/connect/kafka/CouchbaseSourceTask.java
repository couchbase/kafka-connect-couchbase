/**
 * Copyright 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.ExpirationMessage;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.connect.kafka.dcp.EventType;
import com.couchbase.connect.kafka.util.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CouchbaseSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceConnector.class);

    private static final long MAX_TIMEOUT = 10000L;

    private CouchbaseSourceConnectorConfig config;
    private Map<String, String> configProperties;
    private CouchbaseMonitorThread couchbaseMonitorThread;
    private BlockingQueue<DCPRequest> queue;
    private String topic;
    private String bucket;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            configProperties = properties;
            config = new CouchbaseSourceTaskConfig(configProperties);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start CouchbaseSourceTask due to configuration error", e);
        }

        topic = config.getString(CouchbaseSourceConnectorConfig.TOPIC_NAME_CONFIG);
        bucket = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_CONFIG);
        Password password = config.getPassword(CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG);
        String clusterAddress = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_CONFIG);
        long connectionTimeout = config.getLong(CouchbaseSourceConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG);

        queue = new LinkedBlockingQueue<DCPRequest>();
        couchbaseMonitorThread = new CouchbaseMonitorThread(clusterAddress, bucket, password, connectionTimeout, queue);
        couchbaseMonitorThread.start();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> results = new ArrayList<SourceRecord>();

        while (true) {
            DCPRequest event = queue.poll(100, TimeUnit.MILLISECONDS);
            if (event != null) {
                SourceRecord record = convert(event);
                if (record != null) {
                    results.add(record);
                }
            } else if (!results.isEmpty()) {
                LOGGER.info("Poll returns {} result(s)", results.size());
                return results;
            }
        }
    }

    public SourceRecord convert(DCPRequest event) {
        EventType type = EventType.of(event);
        if (type != null) {
            Schema schema = EventType.SCHEMAS.get(type);
            Struct record = new Struct(schema);
            switch (type) {
                case MUTATION:
                    MutationMessage mutation = (MutationMessage) event;
                    record.put("partition", mutation.partition());
                    record.put("key", mutation.key());
                    record.put("expiration", mutation.expiration());
                    record.put("flags", mutation.flags());
                    record.put("cas", mutation.cas());
                    record.put("lockTime", mutation.lockTime());
                    record.put("bySeqno", mutation.bySequenceNumber());
                    record.put("revSeqno", mutation.revisionSequenceNumber());
                    byte[] content = new byte[mutation.content().readableBytes()];
                    mutation.content().readBytes(content);
                    record.put("content", content);
                    break;
                case DELETION:
                    RemoveMessage deletion = (RemoveMessage) event;
                    record.put("partition", deletion.partition());
                    record.put("key", deletion.key());
                    record.put("cas", deletion.cas());
                    record.put("bySeqno", deletion.bySequenceNumber());
                    record.put("revSeqno", deletion.revisionSequenceNumber());
                    break;
                case EXPIRATION:
                    ExpirationMessage expiration = (ExpirationMessage) event;
                    record.put("partition", expiration.partition());
                    record.put("key", expiration.key());
                    record.put("cas", expiration.cas());
                    record.put("bySeqno", expiration.bySequenceNumber());
                    record.put("revSeqno", expiration.revisionSequenceNumber());
                    break;
                case SNAPSHOT:
                    SnapshotMarkerMessage snapshot = (SnapshotMarkerMessage) event;
                    record.put("partition", snapshot.partition());
                    record.put("startSeqno", snapshot.startSequenceNumber());
                    record.put("endSeqno", snapshot.endSequenceNumber());
                    record.put("flags", snapshot.flags());
                    // TODO: figure out what offset for snapshot markers mean
                    return null;
            }
            final Map<String, Object> offset = new HashMap<String, Object>(2);
            offset.put("partition", record.getInt16("partition"));
            offset.put("bySeqno", record.getInt64("bySeqno"));
            final Map<String, String> partition = Collections.singletonMap("bucket", bucket);

            return new SourceRecord(partition, offset, topic, schema, record);
        }
        return null;
    }

    @Override
    public void stop() {
        couchbaseMonitorThread.shutdown();
        try {
            couchbaseMonitorThread.join(MAX_TIMEOUT);
        } catch (InterruptedException e) {
            // Ignore, shouldn't be interrupted
        }
    }
}
