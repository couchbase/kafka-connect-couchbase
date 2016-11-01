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

import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.connect.kafka.dcp.Event;
import com.couchbase.connect.kafka.dcp.EventType;
import com.couchbase.connect.kafka.util.Schemas;
import com.couchbase.connect.kafka.util.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CouchbaseSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceTask.class);

    private static final long MAX_TIMEOUT = 10000L;

    private CouchbaseSourceConnectorConfig config;
    private Map<String, String> configProperties;
    private CouchbaseReader couchbaseReader;
    private BlockingQueue<Event> queue;
    private String topic;
    private String bucket;
    private volatile boolean running;

    private static String bufToString(ByteBuf buf) {
        return new String(bufToBytes(buf), CharsetUtil.UTF_8);
    }

    private static byte[] bufToBytes(ByteBuf buf) {
        byte[] bytes;
        bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
    }

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
        String password = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG);
        List<String> clusterAddress = config.getListWorkaround(CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_CONFIG);
        boolean useSnapshots = config.getBoolean(CouchbaseSourceConnectorConfig.USE_SNAPSHOTS_CONFIG);

        long connectionTimeout = config.getLong(CouchbaseSourceConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG);
        List<String> partitionsList = config.getList(CouchbaseSourceTaskConfig.PARTITIONS_CONFIG);

        Short[] partitions = new Short[partitionsList.size()];
        List<Map<String, String>> kafkaPartitions = new ArrayList<Map<String, String>>(1);
        for (int i = 0; i < partitionsList.size(); i++) {
            partitions[i] = Short.parseShort(partitionsList.get(i));
            Map<String, String> kafkaPartition = new HashMap<String, String>(2);
            kafkaPartition.put("bucket", bucket);
            kafkaPartition.put("partition", partitions[i].toString());
            kafkaPartitions.add(kafkaPartition);
        }
        Map<Map<String, String>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(kafkaPartitions);
        SessionState sessionState = new SessionState();
        for (Map<String, String> kafkaPartition : kafkaPartitions) {
            Map<String, Object> offset = offsets.get(kafkaPartition);
            Short partition = Short.parseShort(kafkaPartition.get("partition"));
            PartitionState partitionState = new PartitionState();
            long startSeqno = 0;
            if (offset != null && offset.containsKey("bySeqno")) {
                startSeqno = (Long) offset.get("bySeqno");
            }
            partitionState.setStartSeqno(startSeqno);
            partitionState.setEndSeqno(SessionState.NO_END_SEQNO);
            partitionState.setSnapshotStartSeqno(startSeqno);
            partitionState.setSnapshotEndSeqno(startSeqno);
            sessionState.set(partition, partitionState);
        }

        running = true;
        queue = new LinkedBlockingQueue<Event>();
        couchbaseReader = new CouchbaseReader(clusterAddress, bucket, password, connectionTimeout,
                queue, partitions, sessionState, useSnapshots);
        couchbaseReader.start();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> results = new LinkedList<SourceRecord>();

        while (running) {
            Event event = queue.poll(100, TimeUnit.MILLISECONDS);
            if (event != null) {
                for (ByteBuf message : event) {
                    SourceRecord record = convert(message);
                    if (record != null) {
                        results.add(record);
                    }
                }
                couchbaseReader.acknowledge(event);
                for (ByteBuf message : event) {
                    message.release();
                }
            } else if (!results.isEmpty()) {
                LOGGER.info("Poll returns {} result(s)", results.size());
                return results;
            }
        }
        return results;
    }

    public SourceRecord convert(ByteBuf event) {
        EventType type = EventType.of(event);
        if (type != null) {
            Struct record = new Struct(Schemas.VALUE_DEFAULT_SCHEMA);
            String key;
            long seqno;
            if (DcpMutationMessage.is(event)) {
                key = bufToString(DcpMutationMessage.key(event));
                seqno = DcpMutationMessage.bySeqno(event);
                record.put("event", "mutation");
                record.put("partition", DcpMutationMessage.partition(event));
                record.put("key", key);
                record.put("expiration", DcpMutationMessage.expiry(event));
                record.put("flags", DcpMutationMessage.flags(event));
                record.put("cas", DcpMutationMessage.cas(event));
                record.put("lockTime", DcpMutationMessage.lockTime(event));
                record.put("bySeqno", seqno);
                record.put("revSeqno", DcpMutationMessage.revisionSeqno(event));
                record.put("content", bufToBytes(DcpMutationMessage.content(event)));
            } else if (DcpDeletionMessage.is(event)) {
                key = bufToString(DcpDeletionMessage.key(event));
                seqno = DcpDeletionMessage.bySeqno(event);
                record.put("event", "deletion");
                record.put("partition", DcpDeletionMessage.partition(event));
                record.put("key", key);
                record.put("cas", DcpDeletionMessage.cas(event));
                record.put("bySeqno", seqno);
                record.put("revSeqno", DcpDeletionMessage.revisionSeqno(event));
            } else if (DcpExpirationMessage.is(event)) {
                key = bufToString(DcpExpirationMessage.key(event));
                seqno = DcpExpirationMessage.bySeqno(event);
                record.put("event", "expiration");
                record.put("partition", DcpExpirationMessage.partition(event));
                record.put("key", key);
                record.put("cas", DcpExpirationMessage.cas(event));
                record.put("bySeqno", seqno);
                record.put("revSeqno", DcpExpirationMessage.revisionSeqno(event));
            } else {
                LOGGER.warn("unexpected event type {}", event.getByte(1));
                return null;
            }
            final Map<String, Object> offset = new HashMap<String, Object>(2);
            offset.put("bySeqno", seqno);
            final Map<String, String> partition = new HashMap<String, String>(2);
            partition.put("bucket", bucket);
            partition.put("partition", record.getInt16("partition").toString());

            return new SourceRecord(partition, offset, topic,
                    Schemas.KEY_SCHEMA, key,
                    Schemas.VALUE_DEFAULT_SCHEMA, record);
        }
        return null;
    }

    @Override
    public void stop() {
        running = false;
        couchbaseReader.shutdown();
        try {
            couchbaseReader.join(MAX_TIMEOUT);
        } catch (InterruptedException e) {
            // Ignore, shouldn't be interrupted
        }
    }
}
