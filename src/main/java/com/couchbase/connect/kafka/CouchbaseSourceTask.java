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

import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.connect.kafka.converter.Converter;
import com.couchbase.connect.kafka.dcp.Event;
import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.util.Version;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private Filter filter;
    private Converter converter;

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

        filter = createFilter(config.getString(CouchbaseSourceConnectorConfig.EVENT_FILTER_CLASS_CONFIG));
        converter = createConverter(
                config.getString(CouchbaseSourceConnectorConfig.DCP_MESSAGE_CONVERTER_CLASS_CONFIG));

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

    private Converter createConverter(final String className) {
        try {
            return Utils.newInstance(className, Converter.class);
        } catch (ClassNotFoundException e) {
            throw new ConnectException("Couldn't create message converter", e);
        }
    }

    private Filter createFilter(final String className) {
        if (className != null && !"".equals(className)) {
            try {
                return Utils.newInstance(className, Filter.class);
            } catch (ClassNotFoundException e) {
                throw new ConnectException("Couldn't create filter in CouchbaseSourceTask due to an error", e);
            }
        }
        return null;
    }

    @Override
    public List<SourceRecord> poll()
            throws InterruptedException {
        List<SourceRecord> results = new LinkedList<SourceRecord>();

        while (running) {
            Event event = queue.poll(100, TimeUnit.MILLISECONDS);
            if (event != null) {

                for (ByteBuf message : event) {
                    if (filter == null || filter.pass(message)) {
                        SourceRecord record = convert(message);
                        if (record != null) {
                            results.add(record);
                        }
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
        return converter.convert(event, bucket, topic);
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
