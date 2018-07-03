/*
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

import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.logging.RedactionLevel;
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.connect.kafka.converter.Converter;
import com.couchbase.connect.kafka.dcp.Event;
import com.couchbase.connect.kafka.dcp.Snapshot;
import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.CouchbaseSourceRecord;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.LegacySourceHandlerAdapter;
import com.couchbase.connect.kafka.handler.source.SourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.couchbase.connect.kafka.util.Version;
import com.couchbase.connect.kafka.util.config.DurationParser;
import com.couchbase.connect.kafka.util.config.Password;
import com.couchbase.connect.kafka.util.config.SizeParser;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    private BlockingQueue<Throwable> errorQueue;
    private String topic;
    private String bucket;
    private volatile boolean running;
    private Filter filter;
    private SourceHandler sourceHandler;
    private int batchSizeMax;
    private boolean connectorNameInOffsets;

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

        RedactionLevel redactionLevel = config.getEnum(RedactionLevel.class, CouchbaseSourceConnectorConfig.LOG_REDACTION_CONFIG);
        CouchbaseLoggerFactory.setRedactionLevel(redactionLevel);

        filter = createFilter(config.getString(CouchbaseSourceConnectorConfig.EVENT_FILTER_CLASS_CONFIG));
        sourceHandler = createHandler(
                config.getString(CouchbaseSourceConnectorConfig.DCP_MESSAGE_CONVERTER_CLASS_CONFIG));

        topic = config.getString(CouchbaseSourceConnectorConfig.TOPIC_NAME_CONFIG);
        bucket = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_CONFIG);
        connectorNameInOffsets = config.getBoolean(CouchbaseSourceConnectorConfig.COMPAT_NAMES_CONFIG);
        String username = config.getUsername();
        String password = Password.CONNECTION.get(config);
        List<String> clusterAddress = config.getList(CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_CONFIG);
        boolean useSnapshots = config.getBoolean(CouchbaseSourceConnectorConfig.USE_SNAPSHOTS_CONFIG);
        boolean sslEnabled = config.getBoolean(CouchbaseSourceConnectorConfig.CONNECTION_SSL_ENABLED_CONFIG);
        String sslKeystoreLocation = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_LOCATION_CONFIG);
        String sslKeystorePassword = Password.SSL_KEYSTORE.get(config);
        batchSizeMax = config.getInt(CouchbaseSourceConnectorConfig.BATCH_SIZE_MAX_CONFIG);
        StreamFrom streamFrom = config.getEnum(StreamFrom.class, CouchbaseSourceConnectorConfig.STREAM_FROM_CONFIG);
        CompressionMode compressionMode = config.getEnum(CompressionMode.class, CouchbaseSourceConnectorConfig.COMPRESSION_CONFIG);

        final long persistencePollingIntervalMillis = DurationParser.parseDuration(
                config.getString(CouchbaseSourceConnectorConfig.PERSISTENCE_POLLING_INTERVAL_CONFIG),
                TimeUnit.MILLISECONDS);
        final int flowControlBufferBytes = (int) Math.min(Integer.MAX_VALUE,
                SizeParser.parseSizeBytes(config.getString(CouchbaseSourceConnectorConfig.FLOW_CONTROL_BUFFER_CONFIG)));

        long connectionTimeout = config.getLong(CouchbaseSourceConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG);
        Short[] partitions = toBoxedShortArray(config.getList(CouchbaseSourceTaskConfig.PARTITIONS_CONFIG));

        Map<Short, Long> partitionToSavedSeqno = readSourceOffsets(partitions);

        running = true;
        queue = new LinkedBlockingQueue<Event>();
        errorQueue = new LinkedBlockingQueue<Throwable>(1);
        couchbaseReader = new CouchbaseReader(clusterAddress, bucket, username, password, connectionTimeout,
                queue, errorQueue, partitions, partitionToSavedSeqno, streamFrom, useSnapshots, sslEnabled, sslKeystoreLocation, sslKeystorePassword,
                compressionMode, persistencePollingIntervalMillis, flowControlBufferBytes);
        couchbaseReader.start();
    }

    @SuppressWarnings("deprecation")
    private SourceHandler createHandler(final String className) {
        try {
            try {
                return Utils.newInstance(className, SourceHandler.class);
            } catch (ClassCastException e) {
                SourceHandler adapter = new LegacySourceHandlerAdapter(Utils.newInstance(className, Converter.class));
                LOGGER.warn("Converter class {} implements deprecated {}. Please update the converter to extend {} instead.",
                        className, Converter.class, SourceHandler.class);
                return adapter;
            }
        } catch (ClassNotFoundException e) {
            throw new ConnectException("Couldn't create message handler", e);
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
        int batchSize = batchSizeMax;

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

                event.ack();
                for (ByteBuf message : event) {
                    message.release();
                }
                batchSize--;
            }
            if (!results.isEmpty() &&
                    (batchSize == 0 || event == null || event instanceof Snapshot)) {
                LOGGER.info("Poll returns {} result(s)", results.size());
                return results;
            }
            if (!errorQueue.isEmpty()) {
                throw new ConnectException(errorQueue.poll());
            }
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    public SourceRecord convert(ByteBuf event) {
        final long vBucketUuid = couchbaseReader.getVBucketUuid(MessageUtil.getVbucket(event));
        final DocumentEvent docEvent = DocumentEvent.create(event, bucket, vBucketUuid);

        CouchbaseSourceRecord r = sourceHandler.handle(new SourceHandlerParams(docEvent, topic));
        if (r == null) {
            return null;
        }

        return new SourceRecord(
                sourcePartition(docEvent.vBucket()),
                sourceOffset(docEvent.bySeqno()),
                r.topic() == null ? topic : r.topic(),
                r.kafkaPartition(),
                r.keySchema(), r.key(),
                r.valueSchema(), r.value(),
                r.timestamp());
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

    /**
     * Loads as many of the requested source offsets as possible.
     * See the caveats for {@link org.apache.kafka.connect.storage.OffsetStorageReader#offsets(Collection)}.
     *
     * @return a map of partitions to sequence numbers.
     */
    private Map<Short, Long> readSourceOffsets(Short[] partitions) {
        Map<Short, Long> partitionToSequenceNumber = new HashMap<Short, Long>();

        Map<Map<String, Object>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(
                sourcePartitions(partitions));

        LOGGER.debug("Raw source offsets: {}", offsets);

        for (Map.Entry<Map<String, Object>, Map<String, Object>> entry : offsets.entrySet()) {
            Map<String, Object> partitionIdentifier = entry.getKey();
            Map<String, Object> offset = entry.getValue();
            if (offset == null) {
                continue;
            }
            Short partition = Short.valueOf((String) partitionIdentifier.get("partition"));
            partitionToSequenceNumber.put(partition, (Long) offset.get("bySeqno"));
        }

        LOGGER.debug("Partition to saved seqno: {}", partitionToSequenceNumber);

        return partitionToSequenceNumber;
    }

    private List<Map<String, Object>> sourcePartitions(Short[] partitions) {
        List<Map<String, Object>> sourcePartitions = new ArrayList<Map<String, Object>>();
        for (Short partition : partitions) {
            sourcePartitions.add(sourcePartition(partition));
        }
        return sourcePartitions;
    }

    /**
     * Converts a Couchbase DCP partition (also known as a vBucket) into the Map format required by Kafka Connect.
     */
    private Map<String, Object> sourcePartition(short partition) {
        final Map<String, Object> sourcePartition = new HashMap<String, Object>(3);
        sourcePartition.put("bucket", bucket);
        sourcePartition.put("partition", String.valueOf(partition)); // Stringify for robust round-tripping across Kafka [de]serialization
        if (connectorNameInOffsets) {
            sourcePartition.put("connector", config.getConnectorName());
        }
        return sourcePartition;
    }

    /**
     * Converts a Couchbase DCP sequence number into the Map format required by Kafka Connect.
     */
    private static Map<String, Object> sourceOffset(long sequenceNumber) {
        return Collections.<String, Object>singletonMap("bySeqno", sequenceNumber);
    }

    private static Short[] toBoxedShortArray(Collection<String> stringifiedShorts) {
        Short[] shortArray = new Short[stringifiedShorts.size()];
        int index = 0;
        for (String s : stringifiedShorts) {
            shortArray[index++] = Short.valueOf(s);
        }
        return shortArray;
    }
}
