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

import com.couchbase.client.core.env.CouchbaseThreadFactory;
import com.couchbase.client.core.logging.LogRedaction;
import com.couchbase.client.core.util.NanoTimestamp;
import com.couchbase.client.dcp.core.logging.RedactionLevel;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.util.PartitionSet;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceTaskConfig;
import com.couchbase.connect.kafka.filter.AllPassFilter;
import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.CollectionMetadata;
import com.couchbase.connect.kafka.handler.source.CouchbaseHeaderSetter;
import com.couchbase.connect.kafka.handler.source.CouchbaseSourceRecord;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.MultiSourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.couchbase.connect.kafka.handler.source.SourceRecordBuilder;
import com.couchbase.connect.kafka.util.ConnectHelper;
import com.couchbase.connect.kafka.util.FirstCallTracker;
import com.couchbase.connect.kafka.util.config.LookupTable;
import com.couchbase.connect.kafka.util.ScopeAndCollection;
import com.couchbase.connect.kafka.util.TopicMap;
import com.couchbase.connect.kafka.util.Version;
import com.couchbase.connect.kafka.util.Watchdog;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

import static com.couchbase.client.core.util.CbStrings.emptyToNull;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static com.couchbase.connect.kafka.CouchbaseReader.isSyntheticInitialOffsetTombstone;
import static com.couchbase.connect.kafka.util.JmxHelper.newJmxMeterRegistry;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CouchbaseSourceTask extends SourceTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceTask.class);
  private static final long STOP_TIMEOUT_MILLIS = SECONDS.toMillis(10);

  private static final ThreadFactory cleanupThreadFactory = new CouchbaseThreadFactory("cb-source-task-cleanup-");

  private String connectorName;
  private volatile CouchbaseReader couchbaseReader; // volatile because non-final and referenced by cleanup thread
  private BlockingQueue<DocumentChange> queue;
  private BlockingQueue<Throwable> errorQueue;
  private LookupTable<ScopeAndCollection, String> topicTemplate;
  private String bucket;
  private Filter filter;

  private LookupTable<ScopeAndCollection, JsonPath> jsonPaths;
  private static final JsonPath ROOT_JSON_PATH = JsonPath.compile("$");
  public static JsonPath parseJsonpath(String s) {
    return s.isEmpty() || s.equals("$") ? ROOT_JSON_PATH : JsonPath.compile(s);
  }
  private static final Configuration jsonpathConf = Configuration.builder()
      .jsonProvider(new JacksonJsonProvider())
      .mappingProvider(new JacksonMappingProvider())
      .options(
          Option.AS_PATH_LIST,
          Option.SUPPRESS_EXCEPTIONS
      )
      .build();

  private boolean filterIsNoop;
  private MultiSourceHandler sourceHandler;
  private CouchbaseHeaderSetter headerSetter;
  private int batchSizeMax;
  private boolean connectorNameInOffsets;
  private boolean noValue;
  private SourceDocumentLifecycle lifecycle;

  private volatile MeterRegistry meterRegistry; // volatile because non-final and referenced by cleanup thread
  private Counter filteredCounter;
  private Timer handlerTimer;
  private Timer filterTimer;
  private Timer timeBetweenPollsTimer;
  private NanoTimestamp endOfLastPoll;

  // Guard against race condition where the framework calls stop() before start() is complete,
  // which could happen prior to the fix for https://issues.apache.org/jira/browse/KAFKA-10792
  private final CountDownLatch startupComplete = new CountDownLatch(1);

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private Optional<String> blackHoleTopic;
  private Optional<String> initialOffsetTopic;

  private final SourceTaskLifecycle taskLifecycle = new SourceTaskLifecycle();
  private final Watchdog watchdog = new Watchdog(taskLifecycle.taskUuid());

  private final FirstCallTracker start = new FirstCallTracker();
  private final FirstCallTracker cleanup = new FirstCallTracker();

  private String taskUuid() {
    return taskLifecycle.taskUuid();
  }

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
    if (start.alreadyCalled()) {
      throw new IllegalStateException("This source task's start() method has already been called; this violates an important assumption about how the Kafka Connect framework manages the SourceTask lifecycle. taskUuid=" + taskUuid());
    }

    try {
      watchdog.start();

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

      String taskNumber = ConnectHelper.getTaskIdFromLoggingContext().orElse(config.maybeTaskId());

      this.meterRegistry = newMeterRegistry(connectorName, taskNumber, config);
      this.handlerTimer = meterRegistry.timer("handler");
      this.filterTimer = meterRegistry.timer("filter");
      this.filteredCounter = meterRegistry.counter("filtered.out");
      this.timeBetweenPollsTimer = meterRegistry.timer("time.between.polls");

      LogRedaction.setRedactionLevel(config.logRedaction());
      RedactionLevel.set(toDcp(config.logRedaction()));

      Map<String, String> unmodifiableProperties = unmodifiableMap(properties);

      lifecycle = SourceDocumentLifecycle.create(taskUuid(), config);

      jsonPaths = config.jsonpathFilter()
          .mapKeys(ScopeAndCollection::parse)
          .mapValues(CouchbaseSourceTask::parseJsonpath);

      filter = Utils.newInstance(config.eventFilter());
      filter.init(unmodifiableProperties);
      filterIsNoop = filter.getClass().equals(AllPassFilter.class); // not just instanceof, because user could do something silly like extend AllPassFilter.

      sourceHandler = createSourceHandler(config);
      sourceHandler.init(unmodifiableProperties);

      headerSetter = new CouchbaseHeaderSetter(config.headerNamePrefix(), config.headers());

      blackHoleTopic = Optional.ofNullable(emptyToNull(config.blackHoleTopic().trim()));
      initialOffsetTopic = Optional.ofNullable(emptyToNull(config.initialOffsetTopic().trim()));

      topicTemplate = config.topic().mapKeys(ScopeAndCollection::parse)
          .withUnderlay(TopicMap.parseCollectionToTopic(config.collectionToTopic()));

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
      couchbaseReader = new CouchbaseReader(config, connectorName, taskNumber, queue, errorQueue, partitions, partitionToSavedSeqno, lifecycle, meterRegistry, initialOffsetTopic.isPresent(), taskLifecycle);
      couchbaseReader.start();

      endOfLastPoll = NanoTimestamp.now();
      watchdog.enterState("started");

    } catch (Exception e) {
      LOGGER.info("Scheduling cleanup because task failed to start. taskUuid={}", taskUuid(), e);
      startupComplete.countDown(); // just so cleanup() doesn't complain about stop() being called before start() finishes.
      cleanup();
      throw e;

    } finally {
      startupComplete.countDown();
    }
  }

  private static MeterRegistry newMeterRegistry(String connectorName, String taskId, CouchbaseSourceTaskConfig config) {
    LinkedHashMap<String, String> commonKeyProperties = new LinkedHashMap<>();
    commonKeyProperties.put("connector", ObjectName.quote(connectorName));
    commonKeyProperties.put("task", taskId);
    MeterRegistry jmx = newJmxMeterRegistry("kafka.connect.couchbase", commonKeyProperties);

    CompositeMeterRegistry composite = new CompositeMeterRegistry();
    composite.add(jmx);
    Optional.ofNullable(newLoggingMeterRegistry(config)).ifPresent(composite::add);

    return composite;
  }

  private static @Nullable MeterRegistry newLoggingMeterRegistry(CouchbaseSourceTaskConfig config) {
    Duration interval = config.metricsInterval();
    String configKey = ConfigHelper.keyName(CouchbaseSourceTaskConfig.class, CouchbaseSourceTaskConfig::metricsInterval);

    if (interval.isZero()) {
      LOGGER.info("Metrics logging is disabled because config property '" + configKey + "' is set to 0.");
      return null;

    } else {
      String metricsCategory = "com.couchbase.connect.kafka.metrics";
      Logger metricsLogger = LoggerFactory.getLogger(metricsCategory);
      LOGGER.info("Will log metrics to logging category '" + metricsCategory + "' at interval: " + interval);

      // Don't need to set "connector" or "task" tags because this info is already in the
      // "connector.context" Mapped Diagnostic Context (MDC) attribute, which is included in the
      // Kafka Connect logging pattern by default.
      return LoggingMeterRegistry
          .builder(k -> "logging.step".equals(k) ? interval.toMillis() + "ms" : null)
          .loggingSink(metricsLogger::info)
          .build();
    }
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
  public void stop() {
    taskLifecycle.logTaskStopped();
    LOGGER.info("Scheduling cleanup because task was asked to stop. taskUuid={}", taskUuid());
    cleanup();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    timeBetweenPollsTimer.record(endOfLastPoll.elapsed());
    watchdog.enterState("polling");

    try {
      // If a fatal error occurred in another thread, propagate it.
      checkErrorQueue();

      // Block until at least one item is available or until the
      // courtesy timeout expires, giving the framework a chance
      // to pause the connector.
      DocumentChange firstEvent = queue.poll(1, SECONDS);
      if (firstEvent == null) {
        LOGGER.debug("Poll returns 0 results; taskUuid={}", taskUuid());
        watchdog.enterState("waiting for next poll (after 0 records)");
        return null; // Looks weird, but caller expects it.
      }

      List<DocumentChange> events = new ArrayList<>();
      try {
        watchdog.enterState("draining queue");
        events.add(firstEvent);
        queue.drainTo(events, batchSizeMax - 1);

        watchdog.enterState("converting to source records (" + events.size() + " events)");
        ConversionResult results = convertToSourceRecords(events);

        filteredCounter.increment(results.dropped);
        if (results.synthetic > 0) {
          LOGGER.info("Poll returns {} result(s) ({} synthetic; filtered out {}); taskUuid={}", results.published + results.synthetic, results.synthetic, results.dropped, taskUuid());
        } else {
          LOGGER.info("Poll returns {} result(s) (filtered out {}); taskUuid={}", results.published, results.dropped, taskUuid());
        }

        watchdog.enterState("waiting for next poll (after " + results.records.size() + " records)");
        return results.records;

      } finally {
        events.forEach(DocumentChange::flowControlAck);
      }
    } catch (Throwable t) {
      watchdog.enterState("polling reported error: " + t);
      throw t;
    } finally {
      endOfLastPoll = NanoTimestamp.now();
    }
  }

  private static class ConversionResult {
    public final List<SourceRecord> records;
    public final int published; // excluding those published to black hole (those are included in "dropped")
    public final int dropped;
    public final int synthetic;

    public ConversionResult(List<SourceRecord> records, int published, int dropped, int synthetic) {
      this.records = records;
      this.published = published;
      this.dropped = dropped;
      this.synthetic = synthetic;
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
      LOGGER.warn("Committed a record we didn't create? Record key {}; taskUuid={}", record.key(), taskUuid());
    }

    sourceHandler.onRecordCommitted(record, metadata);
  }

  private void checkErrorQueue() throws ConnectException {
    final Throwable fatalError = errorQueue.poll();
    if (fatalError != null) {
      throw new ConnectException(fatalError);
    }
  }

  private static boolean jsonpathMatch(JsonPath filter, byte[] document) {
    try {
      if (filter == ROOT_JSON_PATH) {
        return true;
      }
      List<?> result = filter.read(new ByteArrayInputStream(document), jsonpathConf);
      return !result.isEmpty();
    } catch (InvalidJsonException | IOException e) {
      return false;
    }
  }

  private String getDefaultTopic(DocumentEvent docEvent) {
    ScopeAndCollection scopeAndCollection = scopeAndCollection(docEvent);
    String topic = topicTemplate.get(scopeAndCollection);

    if (topic.contains("${")) {
      return topic
          .replace("${bucket}", bucket)
          .replace("${scope}", scopeAndCollection.getScope())
          .replace("${collection}", scopeAndCollection.getCollection())
          .replace("%", "_"); // % is valid in Couchbase name but not Kafka topic name
    }

    return topic;
  }

  private ConversionResult convertToSourceRecords(List<DocumentChange> events) {
    List<SourceRecord> results = new ArrayList<>(events.size());
    int dropped = 0;
    int initialOffsets = 0;

    for (DocumentChange e : events) {
      DocumentEvent docEvent = DocumentEvent.create(e, bucket);

      // Handle synthetic tombstones separately to ensure they
      // don't get dropped by filters
      //
      // We could assume the topic config is always present, but maybe a trickster
      // inserted a real document with the same key as a synthetic tombstone.
      if (isSyntheticInitialOffsetTombstone(e) && initialOffsetTopic.isPresent()) {
        String topic = initialOffsetTopic.get();
        SourceRecord sourceRecord = createSourceOffsetUpdateRecord(e.getKey(), topic, e);
        lifecycle.logConvertedToKafkaRecord(e, sourceRecord);
        results.add(sourceRecord);
        initialOffsets++;
        continue;
      }

      // Pre-filter with jsonpath
      boolean jsonpathPassed = jsonpathMatch(jsonPaths.get(
          ScopeAndCollection.parse(docEvent.collectionMetadata().scopeName() + "." + docEvent.collectionMetadata().collectionName())),
          docEvent.content()
      );
      if (!jsonpathPassed) {
        lifecycle.logSkippedBecauseJsonpathFilterSaysIgnore(e);
        dropped++;
        blackHoleTopic.ifPresent(topic -> results.add(createSourceOffsetUpdateRecord(topic, e)));
        continue;
      }

      // Don't record filter timings unless the filter is actually doing something.
      boolean passed = filterIsNoop || filterTimer.record(() -> filter.pass(docEvent));
      if (!passed) {
        lifecycle.logSkippedBecauseFilterSaysIgnore(e);
        dropped++;
        blackHoleTopic.ifPresent(topic -> results.add(createSourceOffsetUpdateRecord(topic, e)));
        continue;
      }

      List<CouchbaseSourceRecord> sourceRecords = convertToSourceRecords(e, docEvent);
      if (sourceRecords.isEmpty()) {
        lifecycle.logSkippedBecauseHandlerSaysIgnore(e);
        dropped++;
        blackHoleTopic.ifPresent(topic -> results.add(createSourceOffsetUpdateRecord(topic, e)));
        continue;
      }

      sourceRecords.forEach(it -> lifecycle.logConvertedToKafkaRecord(e, it));
      results.addAll(sourceRecords);
    }

    int published = results.size() - initialOffsets;
    if (blackHoleTopic.isPresent()) {
      published -= dropped;
    }

    return new ConversionResult(results, published, dropped, initialOffsets);
  }

  /**
   * Returns a new synthetic record representing a Couchbase event
   * that was dropped by the filter or source handler.
   * <p>
   * These records are published to the configured "black hole" topic
   * as a way to tell Kafka Connect about the source offset of the ignored event.
   */
  private SourceRecord createSourceOffsetUpdateRecord(String topic, DocumentChange change) {
    // Include vbucket in key so records aren't all assigned to the same Kafka partition
    String key = "ignored-" + change.getVbucket();
    return createSourceOffsetUpdateRecord(key, topic, change);
  }

  private SourceRecord createSourceOffsetUpdateRecord(String key, String topic, DocumentChange change) {
    return new SourceRecordBuilder()
        .key(key)
        .build(
            change,
            sourcePartition(change.getVbucket()),
            sourceOffset(change),
            topic
        );
  }

  private List<CouchbaseSourceRecord> convertToSourceRecords(DocumentChange change, DocumentEvent docEvent) {
    String topic = getDefaultTopic(docEvent);

    List<SourceRecordBuilder> builders = handlerTimer.record(() ->
        sourceHandler.convertToSourceRecords(new SourceHandlerParams(docEvent, topic, noValue))
    );

    requireNonNull(builders, "The source handler's convertToSourceRecords() method returned null instead of a List; this is forbidden.");

    if (builders.isEmpty()) {
      return emptyList();
    }

    List<CouchbaseSourceRecord> result = new ArrayList<>(builders.size());
    for (SourceRecordBuilder builder : builders) {
      requireNonNull(builder, "The source handler's convertToSourceRecords() method returned a list containing a null item; this is forbidden.");

      headerSetter.setHeaders(builder.headers(), docEvent);
      CouchbaseSourceRecord record = builder.build(
          change,
          sourcePartition(docEvent.partition()),
          sourceOffset(change),
          topic
      );
      result.add(record);
    }

    return result;
  }

  private void cleanup() {
    // The framework may call stop() multiple times. Only need to clean up once.
    if (cleanup.alreadyCalled()) {
      LOGGER.info("Ignoring redundant cleanup request; taskUuid={}", taskUuid());
      return;
    }

    // The connector's stop() method must return quickly. Do the actual cleanup work in a separate thread
    // because it can take a while to ensure an orderly shutdown of the DCP client.
    cleanupThreadFactory.newThread(() -> {
      try {
        Thread.currentThread().setName(Thread.currentThread().getName() + "-" + taskUuid());

        if (startupComplete.getCount() != 0) {
          // Prior to the fix for https://issues.apache.org/jira/browse/KAFKA-10792
          // the framework could call stop() on a separate thread from start(),
          // potentially before start() finished.

          LOGGER.info("Task was asked to stop before it finished starting; deferring cleanup until start() completes. taskUuid={}", taskUuid());
          // The framework should never call stop without also calling start, but out of paranoia let's not bank on it.
          // In practice, we won't have to wait much longer than the bootstrap timeout which is _well_ under the safeguard timeout specified here.
          Duration safeguardTimeout = Duration.ofMinutes(30);
          if (!startupComplete.await(safeguardTimeout.toMillis(), MILLISECONDS)) {
            LOGGER.error("This task's start() method did not complete within the safeguard timeout of {}. Making a last-ditch effort to clean up. taskUuid={}", safeguardTimeout, taskUuid());
          }
        }

        LOGGER.info("Cleaning up now; taskUuid={}", taskUuid());

        this.watchdog.stop();

        if (this.meterRegistry != null) {
          this.meterRegistry.close();
        }

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
      } catch (Throwable t) {
        LOGGER.error("Error while cleaning up resources; taskUuid={}", taskUuid(), t);

      } finally {
        taskLifecycle.logTaskCleanupComplete();
      }
    }).start();
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

    LOGGER.debug("Raw source offsets: {}; taskUuid={}", offsets, taskUuid());

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
      LOGGER.error("Offset storage reader returned no information about these partitions: {}; taskUuid={}",
          PartitionSet.from(missingPartitions), taskUuid()
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

  @NullMarked
  private static MultiSourceHandler createSourceHandler(CouchbaseSourceTaskConfig config) {
    Object handlerObject = Utils.newInstance(config.sourceHandler());

    if (handlerObject instanceof MultiSourceHandler) {
      return (MultiSourceHandler) handlerObject;
    }

    if (!(handlerObject instanceof SourceHandler)) {
      String configKey = ConfigHelper.keyName(CouchbaseSourceTaskConfig.class, CouchbaseSourceTaskConfig::sourceHandler);
      throw new ConfigException(
          "Invalid value for connector config property '" + configKey + "' ;" +
          " Source handler must be an instance of " + SourceHandler.class.getName()
              + " or " + MultiSourceHandler.class.getName()
              + ", but got: " + handlerObject.getClass().getName());
    }

    SourceHandler sourceHandler = (SourceHandler) handlerObject;
    return new MultiSourceHandler() {
      @Override
      public void init(Map<String, String> configProperties) {
        sourceHandler.init(configProperties);
      }

      @Override
      public List<SourceRecordBuilder> convertToSourceRecords(SourceHandlerParams params) {
        SourceRecordBuilder result = sourceHandler.handle(params);
        return result == null ? emptyList() : singletonList(result);
      }

      @Override
      public void onRecordCommitted(SourceRecord record, @Nullable RecordMetadata metadata) {
        sourceHandler.onRecordCommitted(record, metadata);
      }
    };
  }

}
