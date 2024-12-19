/*
 * Copyright 2021 Couchbase, Inc.
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

import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.metrics.LogLevel;
import com.couchbase.connect.kafka.config.common.LoggingConfig;
import com.couchbase.connect.kafka.handler.source.CouchbaseSourceRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.connect.kafka.util.ConnectHelper.getConnectorContextFromLoggingContext;
import static com.couchbase.connect.kafka.util.ConnectHelper.getTaskIdFromLoggingContext;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.util.Collections.emptyMap;

/**
 * Logs document lifecycle events.
 */
public class SourceDocumentLifecycle {

  public enum Milestone {
    RECEIVED_FROM_COUCHBASE,

    SKIPPED_BECAUSE_FILTER_SAYS_IGNORE,
    SKIPPED_BECAUSE_HANDLER_SAYS_IGNORE,

    CONVERTED_TO_KAFKA_RECORD,
    COMMITTED_TO_TOPIC,

    // For an ignored event, we published a dummy message to the configured black hole topic
    // to tell Kafka Connect about the ignored event's source offset.
    SOURCE_OFFSET_UPDATE_COMMITTED_TO_BLACK_HOLE_TOPIC,
    ;
  }

  private static final Logger log = LoggerFactory.getLogger(SourceDocumentLifecycle.class);

  private final String taskId = getTaskIdFromLoggingContext().orElse("?");
  private final LogLevel logLevel;

  public boolean enabled() {
    return logLevel.isEnabled(log);
  }

  public static SourceDocumentLifecycle create(LoggingConfig config) {
    return new SourceDocumentLifecycle(config);
  }

  private SourceDocumentLifecycle(LoggingConfig config) {
    this.logLevel = config.logDocumentLifecycle() ? LogLevel.INFO : LogLevel.DEBUG;
    log.info("Logging document lifecycle milestones to this category at {} level", logLevel);
  }

  public void logReceivedFromCouchbase(DocumentChange event) {
    if (enabled()) {
      LinkedHashMap<String, Object> details = new LinkedHashMap<>();
      details.put("connectTaskId", taskId);
      details.put("revision", event.getRevision());
      details.put("type", event.isMutation() ? "mutation" : "deletion");
      details.put("partition", event.getVbucket());
      details.put("sequenceNumber", event.getOffset().getSeqno());
      details.put("sizeInBytes", event.getContent().length);
      details.put("usSinceCouchbaseChange(might be inaccurate before Couchbase 7)", event.getTimestamp().until(Instant.now(), MICROS));
      logMilestone(event, Milestone.RECEIVED_FROM_COUCHBASE, details);
    }
  }

  public void logSkippedBecauseFilterSaysIgnore(DocumentChange event) {
    logMilestone(event, Milestone.SKIPPED_BECAUSE_FILTER_SAYS_IGNORE);
  }

  public void logSkippedBecauseHandlerSaysIgnore(DocumentChange event) {
    logMilestone(event, Milestone.SKIPPED_BECAUSE_HANDLER_SAYS_IGNORE);
  }

  public void logConvertedToKafkaRecord(DocumentChange event, SourceRecord record) {
    if (enabled()) {
      LinkedHashMap<String, Object> details = new LinkedHashMap<>();
      details.put("topic", record.topic());
      details.put("key", record.key());
      details.put("kafkaPartition", record.kafkaPartition());
      details.put("sourcePartition", record.sourcePartition());
      details.put("sourceOffset", record.sourceOffset());
      logMilestone(event, Milestone.CONVERTED_TO_KAFKA_RECORD, details);
    }
  }

  public void logCommittedToKafkaTopic(CouchbaseSourceRecord sourceRecord, @Nullable RecordMetadata metadata) {
    logMilestone(sourceRecord, Milestone.COMMITTED_TO_TOPIC, toMap(metadata));
  }

  public void logSourceOffsetUpdateCommittedToBlackHoleTopic(CouchbaseSourceRecord sourceRecord, @Nullable RecordMetadata metadata) {
    logMilestone(sourceRecord, Milestone.SOURCE_OFFSET_UPDATE_COMMITTED_TO_BLACK_HOLE_TOPIC, toMap(metadata));
  }

  private Map<String, Object> toMap(@Nullable RecordMetadata metadata) {
    LinkedHashMap<String, Object> details = new LinkedHashMap<>();
    if (metadata != null) {
      details.put("topic", metadata.topic());
      details.put("partition", metadata.partition());
      details.put("offset", metadata.offset());
      details.put("timestamp", metadata.timestamp());
      details.put("serializedKeySize", metadata.serializedKeySize());
      details.put("serializedValueSize", metadata.serializedValueSize());
    }
    return mapOf("recordMetadata", details);
  }

  private void logMilestone(DocumentChange event, Milestone milestone) {
    logMilestone(event, milestone, emptyMap());
  }

  private void logMilestone(CouchbaseSourceRecord sourceRecord, Milestone milestone, Map<String, Object> milestoneDetails) {
    if (enabled()) {
      LinkedHashMap<String, Object> message = new LinkedHashMap<>();
      message.put("milestone", milestone);
      message.put("tracingToken", sourceRecord.getTracingToken());

      // In case the user customized their logging config to exclude MDC
      getConnectorContextFromLoggingContext().ifPresent(it -> message.put("context", it));

      message.put("documentId", sourceRecord.getCouchbaseDocumentId());
      message.putAll(milestoneDetails);
      doLog(message);
    }
  }

  private void logMilestone(DocumentChange event, Milestone milestone, Map<String, Object> milestoneDetails) {
    if (enabled()) {
      LinkedHashMap<String, Object> message = new LinkedHashMap<>();
      message.put("milestone", milestone);
      message.put("tracingToken", event.getTracingToken());

      // In case the user customized their logging config to exclude MDC
      getConnectorContextFromLoggingContext().ifPresent(it -> message.put("context", it));

      message.put("documentId", event.getQualifiedKey());
      message.putAll(milestoneDetails);
      doLog(message);
    }
  }

  private void doLog(Object message) {
    try {
      logLevel.log(log, Mapper.encodeAsString(message));
    } catch (Exception e) {
      logLevel.log(log, message.toString());
    }
  }
}
