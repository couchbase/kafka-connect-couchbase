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
import com.couchbase.client.dcp.util.PartitionSet;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceTaskConfig;
import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.CollectionMetadata;
import com.couchbase.connect.kafka.handler.source.CouchbaseSourceRecord;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.SourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.couchbase.connect.kafka.handler.source.SourceRecordBuilder;
import com.couchbase.connect.kafka.util.ScopeAndCollection;
import com.couchbase.connect.kafka.util.TopicMap;
import com.couchbase.connect.kafka.util.Version;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.couchbase.client.core.util.CbStrings.emptyToNull;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CouchbaseSourceTask extends SourceTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceTask.class);
  private static final long STOP_TIMEOUT_MILLIS = SECONDS.toMillis(10);

  private String connectorName;
  private CouchbaseReader couchbaseReader;
  private BlockingQueue<DocumentChange> queue;
  private BlockingQueue<Throwable> errorQueue;
  private String defaultTopicTemplate;
  private Map<ScopeAndCollection, String> collectionToTopic;
  private String bucket;
  private Filter filter;
  private SourceHandler sourceHandler;
  private int batchSizeMax;
  private boolean connectorNameInOffsets;
  private boolean noValue;
  private SourceDocumentLifecycle lifecycle;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<String> blackHoleTopic;

  private final SourceTaskLifecycle taskLifecycle = new SourceTaskLifecycle();

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void initialize(SourceTaskContext context) {
    super.initialize(context);
    taskLifecycle.logTaskInitialized(context.configs().get("name"));
  }

  @Override
  public void commit() throws InterruptedException {
    super.commit();
    taskLifecycle.logOffsetCommitHook();
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

    Map<String, String> unmodifiableProperties = unmodifiableMap(properties);

    lifecycle = SourceDocumentLifecycle.create(config);

    filter = Utils.newInstance(config.eventFilter());
    filter.init(unmodifiableProperties);

    sourceHandler = Utils.newInstance(config.sourceHandler());
    sourceHandler.init(unmodifiableProperties);

    blackHoleTopic = Optional.ofNullable(emptyToNull(config.blackHoleTopic().trim()));

    defaultTopicTemplate = config.topic();
    collectionToTopic = TopicMap.parseCollectionToTopic(config.collectionToTopic());
    bucket = config.bucket();
    //noinspection deprecation
    connectorNameInOffsets = config.connectorNameInOffsets();
    batchSizeMax = config.batchSizeMax();
    noValue = config.noValue();

    PartitionSet partitionSet = PartitionSet.parse(config.partitions());
    taskLifecycle.logTaskStarted(connectorName, partitionSet);

    List<Integer> partitions = partitionSet.toList();
    Map<Integer, SourceOffset> partitionToSavedSeqno = readSourceOffsets(partitions);

    Set<Integer> partitionsWithoutSavedOffsets = new HashSet<>(partitions);
    partitionsWithoutSavedOffsets.removeAll(partitionToSavedSeqno.keySet());

    taskLifecycle.logSourceOffsetsRead(
        partitionToSavedSeqno,
        PartitionSet.from(partitionsWithoutSavedOffsets)
    );

    queue = new LinkedBlockingQueue<>();
    errorQueue = new LinkedBlockingQueue<>(1);
    couchbaseReader = new CouchbaseReader(config, connectorName, queue, errorQueue, partitions, partitionToSavedSeqno, lifecycle);
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
    DocumentChange firstEvent = queue.poll(1, SECONDS);
    if (firstEvent == null) {
      LOGGER.debug("Poll returns 0 results");
      return null; // Looks weird, but caller expects it.
    }

    List<DocumentChange> events = new ArrayList<>();
    try {
      events.add(firstEvent);
      queue.drainTo(events, batchSizeMax - 1);

      ConversionResult results = convertToSourceRecords(events);

      LOGGER.info("Poll returns {} result(s) (filtered out {})", results.published, results.dropped);
      return results.records;

    } finally {
      events.forEach(DocumentChange::flowControlAck);
    }
  }

  private static class ConversionResult {
    public final List<SourceRecord> records;
    public final int published; // excluding those published to black hole (those are included in "dropped")
    public final int dropped;

    public ConversionResult(List<SourceRecord> records, int published, int dropped) {
      this.records = records;
      this.published = published;
      this.dropped = dropped;
    }
  }

  /**
   * Returns true if the record is a synthetic source offset update for
   * an ignored Couchbase event, destined for a black hole.
   */
  private boolean isSourceOffsetUpdate(SourceRecord record) {
    return blackHoleTopic.isPresent() && blackHoleTopic.get().equals(record.topic());
  }

  @Override
  public void commitRecord(SourceRecord record, RecordMetadata metadata) {
    if (record instanceof CouchbaseSourceRecord) {
      CouchbaseSourceRecord couchbaseRecord = (CouchbaseSourceRecord) record;
      if (isSourceOffsetUpdate(couchbaseRecord)) {
        lifecycle.logSourceOffsetUpdateCommittedToBlackHoleTopic(couchbaseRecord, metadata);
      } else {
        lifecycle.logCommittedToKafkaTopic(couchbaseRecord, metadata);
      }
    } else {
      LOGGER.warn("Committed a record we didn't create? Record key {}", record.key());
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
    return defaultTopicTemplate
        .replace("${bucket}", bucket)
        .replace("${scope}", collectionMetadata.scopeName())
        .replace("${collection}", collectionMetadata.collectionName())
        .replace("%", "_"); // % is valid in Couchbase name but not Kafka topic name
  }

  private ConversionResult convertToSourceRecords(List<DocumentChange> events) {
    List<SourceRecord> results = new ArrayList<>(events.size());
    int dropped = 0;
    for (DocumentChange e : events) {
      DocumentEvent docEvent = DocumentEvent.create(e, bucket);

      if (!filter.pass(docEvent)) {
        lifecycle.logSkippedBecauseFilterSaysIgnore(e);
        dropped++;
        blackHoleTopic.ifPresent(topic -> results.add(createSourceOffsetUpdateRecord(topic, e, docEvent)));
        continue;
      }

      SourceRecord sourceRecord = convertToSourceRecord(e, docEvent);
      if (sourceRecord == null) {
        lifecycle.logSkippedBecauseHandlerSaysIgnore(e);
        dropped++;
        blackHoleTopic.ifPresent(topic -> results.add(createSourceOffsetUpdateRecord(topic, e, docEvent)));
        continue;
      }

      lifecycle.logConvertedToKafkaRecord(e, sourceRecord);
      results.add(sourceRecord);
    }

    int published = results.size();
    if (blackHoleTopic.isPresent()) {
      published -= dropped;
    }

    return new ConversionResult(results, published, dropped);
  }

  /**
   * Returns a new synthetic record representing a Couchbase event
   * that was dropped by the filter or source handler.
   * <p>
   * These records are published to the configured "black hole" topic
   * as a way to tell Kafka Connect about the source offset of the ignored event.
   */
  private SourceRecord createSourceOffsetUpdateRecord(String topic, DocumentChange change, DocumentEvent docEvent) {
    return new SourceRecordBuilder()
        // Include vbucket in key so records aren't all assigned to the same Kafka partition
        .key("ignored-" + change.getVbucket())
        .build(
            change,
            sourcePartition(docEvent.partition()),
            sourceOffset(change),
            topic
        );
  }

  private CouchbaseSourceRecord convertToSourceRecord(DocumentChange change, DocumentEvent docEvent) {
    String topic = collectionToTopic.getOrDefault(
        scopeAndCollection(docEvent),
        getDefaultTopic(docEvent)
    );

    SourceRecordBuilder builder = sourceHandler.handle(new SourceHandlerParams(docEvent, topic, noValue));
    if (builder == null) {
      return null;
    }
    return builder.build(change,
        sourcePartition(docEvent.partition()),
        sourceOffset(change),
        topic);
  }

  @Override
  public void stop() {
    taskLifecycle.logTaskStopped();

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
  private Map<Integer, SourceOffset> readSourceOffsets(Collection<Integer> partitions) {
    Map<Map<String, Object>, Map<String, Object>> offsets = context.offsetStorageReader()
        .offsets(sourcePartitions(partitions));

    LOGGER.debug("Raw source offsets: {}", offsets);

    // Remove partitions from this set as we see them in the map. Expect a map entry for each
    // requested partition. A null-valued entry indicates the partition has no saved offset.
    // A *missing* entry indicates something bad happened inside the offset storage reader.
    Set<Integer> missingPartitions = new HashSet<>(partitions);

    // Populated from the map entries with non-null values.
    SortedMap<Integer, SourceOffset> partitionToSourceOffset = new TreeMap<>();

    offsets.forEach((partitionIdentifier, offset) -> {
      int partition = Integer.parseInt((String) partitionIdentifier.get("partition"));
      missingPartitions.remove(partition);

      if (offset != null) {
        partitionToSourceOffset.put(partition, SourceOffset.fromMap(offset));
      }
    });

    if (!missingPartitions.isEmpty()) {
      // Something is wrong with the offset storage reader.
      // We should have seen one entry for each requested partition.
      LOGGER.error("Offset storage reader returned no information about these partitions: {}",
          PartitionSet.from(missingPartitions)
      );
    }

    return partitionToSourceOffset;
  }

  private List<Map<String, Object>> sourcePartitions(Collection<Integer> partitions) {
    List<Map<String, Object>> sourcePartitions = new ArrayList<>();
    for (Integer partition : partitions) {
      sourcePartitions.add(sourcePartition(partition));
    }
    return sourcePartitions;
  }

  /**
   * Converts a Couchbase DCP partition (also known as a vBucket) into the Map format required by Kafka Connect.
   */
  private Map<String, Object> sourcePartition(int partition) {
    final Map<String, Object> sourcePartition = new HashMap<>(3);
    sourcePartition.put("bucket", bucket);
    sourcePartition.put("partition", String.valueOf(partition)); // Stringify for robust round-tripping across Kafka [de]serialization
    if (connectorNameInOffsets) {
      sourcePartition.put("connector", connectorName);
    }
    return sourcePartition;
  }

  /**
   * Converts a Couchbase DCP stream offset into the Map format required by Kafka Connect.
   */
  private static Map<String, Object> sourceOffset(DocumentChange change) {
    return new SourceOffset(change.getOffset()).toMap();
  }

  private static ScopeAndCollection scopeAndCollection(DocumentEvent docEvent) {
    CollectionMetadata md = docEvent.collectionMetadata();
    return new ScopeAndCollection(md.scopeName(), md.collectionName());
  }

}
