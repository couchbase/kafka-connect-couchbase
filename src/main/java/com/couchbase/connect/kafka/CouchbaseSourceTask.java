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

import com.couchbase.client.core.logging.LogRedaction;
import com.couchbase.client.dcp.core.logging.RedactionLevel;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceTaskConfig;
import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.CollectionMetadata;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.SourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.couchbase.connect.kafka.handler.source.SourceRecordBuilder;
import com.couchbase.connect.kafka.util.Version;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class CouchbaseSourceTask extends SourceTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceTask.class);

  private static final long STOP_TIMEOUT_MILLIS = SECONDS.toMillis(10);

  private String connectorName;
  private CouchbaseReader couchbaseReader;
  private BlockingQueue<DocumentChange> queue;
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
    this.connectorName = properties.get("name");

    CouchbaseSourceTaskConfig config;
    try {
      config = ConfigHelper.parse(CouchbaseSourceTaskConfig.class, properties);
      if (isNullOrEmpty(connectorName)) {
        throw new ConfigException("Connector must have a non-blank 'name' config property.");
      }
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start CouchbaseSourceTask due to configuration error", e);
    }

    LogRedaction.setRedactionLevel(config.logRedaction());
    RedactionLevel.set(toDcp(config.logRedaction()));

    filter = Utils.newInstance(config.eventFilter());
    sourceHandler = Utils.newInstance(config.sourceHandler());

    topic = config.topic();
    bucket = config.bucket();
    connectorNameInOffsets = config.connectorNameInOffsets();
    batchSizeMax = config.batchSizeMax();

    Short[] partitions = toBoxedShortArray(config.partitions());
    Map<Short, SeqnoAndVbucketUuid> partitionToSavedSeqno = readSourceOffsets(partitions);

    running = true;
    queue = new LinkedBlockingQueue<>();
    errorQueue = new LinkedBlockingQueue<>(1);
    couchbaseReader = new CouchbaseReader(config, connectorName, queue, errorQueue, partitions, partitionToSavedSeqno);
    couchbaseReader.start();
  }

  private RedactionLevel toDcp(com.couchbase.client.core.logging.RedactionLevel level) {
    switch (level) {
      case FULL:
        return RedactionLevel.FULL;
      case NONE:
        return RedactionLevel.NONE;
      case PARTIAL:
        return RedactionLevel.PARTIAL;
      default:
        throw new IllegalArgumentException("Unrecognized redaction level: " + level);
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    // If a fatal error occurred in another thread, propagate it.
    checkErrorQueue();

    // Block until at least one item is available or until the
    // courtesy timeout expires, giving the framework a chance
    // to pause the connector.
    DocumentChange event = queue.poll(1, SECONDS);
    if (event == null) {
      LOGGER.debug("Poll returns 0 results");
      return null; // Looks weird, but caller expects it.
    }

    List<DocumentChange> events = new ArrayList<>();
    try {
      events.add(event);
      queue.drainTo(events, batchSizeMax - 1);

      List<SourceRecord> results = events.stream()
          .map(e -> DocumentEvent.create(event, bucket))
          .filter(e -> filter.pass(e))
          .map(this::convertToSourceRecord)
          .filter(Objects::nonNull)
          .collect(toList());

      int excluded = events.size() - results.size();
      LOGGER.info("Poll returns {} result(s) (filtered out {})", results.size(), excluded);
      return results;

    } finally {
      events.forEach(DocumentChange::flowControlAck);
    }
  }

  private void checkErrorQueue() throws ConnectException {
    final Throwable fatalError = errorQueue.poll();
    if (fatalError != null) {
      throw new ConnectException(fatalError);
    }
  }

  private String getDefaultTopic(DocumentEvent docEvent) {
    CollectionMetadata collectionMetadata = docEvent.collectionMetadata();
    return topic
        .replace("${bucket}", bucket)
        .replace("${scope}", collectionMetadata.scopeName())
        .replace("${collection}", collectionMetadata.collectionName())
        .replace("%", "_"); // % is valid in Couchbase name but not Kafka topic name
  }

  private SourceRecord convertToSourceRecord(DocumentEvent docEvent) {
    String defaultTopic = getDefaultTopic(docEvent);

    SourceRecordBuilder builder = sourceHandler.handle(new SourceHandlerParams(docEvent, defaultTopic));
    if (builder == null) {
      return null;
    }
    return builder.build(
        sourcePartition(docEvent.partition()),
        sourceOffset(docEvent),
        defaultTopic);
  }

  @Override
  public void stop() {
    running = false;
    if (couchbaseReader != null) {
      couchbaseReader.shutdown();
      try {
        couchbaseReader.join(STOP_TIMEOUT_MILLIS);
        if (couchbaseReader.isAlive()) {
          LOGGER.error("Reader thread is still alive after shutdown request.");
        }
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted while joining reader thread.", e);
      }
    }
  }

  /**
   * Loads as many of the requested source offsets as possible.
   * See the caveats for {@link org.apache.kafka.connect.storage.OffsetStorageReader#offsets(Collection)}.
   *
   * @return a map of partitions to sequence numbers.
   */
  private Map<Short, SeqnoAndVbucketUuid> readSourceOffsets(Short[] partitions) {
    Map<Short, SeqnoAndVbucketUuid> partitionToSequenceNumber = new HashMap<>();

    Map<Map<String, Object>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(
        sourcePartitions(partitions));

    LOGGER.debug("Raw source offsets: {}", offsets);

    for (Map.Entry<Map<String, Object>, Map<String, Object>> entry : offsets.entrySet()) {
      Map<String, Object> partitionIdentifier = entry.getKey();
      Map<String, Object> offset = entry.getValue();
      if (offset == null) {
        continue;
      }
      short partition = Short.parseShort((String) partitionIdentifier.get("partition"));
      long seqno = (Long) offset.get("bySeqno");
      Long vbuuid = (Long) offset.get("vbuuid"); // might be absent if upgrading from older version
      partitionToSequenceNumber.put(partition, new SeqnoAndVbucketUuid(seqno, vbuuid));
    }

    LOGGER.debug("Partition to saved seqno: {}", partitionToSequenceNumber);

    return partitionToSequenceNumber;
  }

  private List<Map<String, Object>> sourcePartitions(Short[] partitions) {
    List<Map<String, Object>> sourcePartitions = new ArrayList<>();
    for (Short partition : partitions) {
      sourcePartitions.add(sourcePartition(partition));
    }
    return sourcePartitions;
  }

  /**
   * Converts a Couchbase DCP partition (also known as a vBucket) into the Map format required by Kafka Connect.
   */
  private Map<String, Object> sourcePartition(short partition) {
    final Map<String, Object> sourcePartition = new HashMap<>(3);
    sourcePartition.put("bucket", bucket);
    sourcePartition.put("partition", String.valueOf(partition)); // Stringify for robust round-tripping across Kafka [de]serialization
    if (connectorNameInOffsets) {
      sourcePartition.put("connector", connectorName);
    }
    return sourcePartition;
  }

  /**
   * Converts a Couchbase DCP sequence number + vBucket UUID into the Map format required by Kafka Connect.
   */
  private static Map<String, Object> sourceOffset(DocumentEvent event) {
    Map<String, Object> offset = new HashMap<>();
    offset.put("bySeqno", event.bySeqno());
    offset.put("vbuuid", event.partitionUuid());
    return offset;
  }

  private static Short[] toBoxedShortArray(Collection<String> stringifiedShorts) {
    return stringifiedShorts.stream()
        .map(Short::valueOf)
        .toArray(Short[]::new);
  }
}
