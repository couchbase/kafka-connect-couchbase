/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka;

import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.dcp.metrics.LogLevel;
import com.couchbase.client.dcp.util.PartitionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.transformValues;
import static com.couchbase.connect.kafka.util.ConnectHelper.getConnectorContextFromLoggingContext;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class SourceTaskLifecycle {

  public enum Milestone {
    TASK_INITIALIZED,
    TASK_STARTED,
    TASK_STOPPED, // stop requested; framework called SourceConnector.stop()
    TASK_CLEANUP_COMPLETE,
    SOURCE_OFFSETS_READ,
    MISSING_SOURCE_OFFSETS_SET_TO_NOW,
    OFFSET_COMMIT_HOOK(LogLevel.DEBUG),
    ;

    private final LogLevel logLevel;

    Milestone() {
      this(LogLevel.INFO);
    }

    Milestone(LogLevel logLevel) {
      this.logLevel = requireNonNull(logLevel);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(SourceTaskLifecycle.class);

  private final String uuid = UUID.randomUUID().toString();

  public String taskUuid() {
    return uuid;
  }

  public void logTaskInitialized(String connectorName) {
    logMilestone(Milestone.TASK_INITIALIZED, mapOf("connectorName", connectorName));
  }

  public void logOffsetCommitHook() {
    logMilestone(Milestone.OFFSET_COMMIT_HOOK, emptyMap());
  }

  public void logTaskStarted(String name, PartitionSet partitions) {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("connectorName", name);
    details.put("assignedPartitions", partitions.format());
    logMilestone(Milestone.TASK_STARTED, details);
  }

  public void logSourceOffsetsRead(Map<Integer, SourceOffset> sourceOffsets, PartitionSet partitionsWithoutSavedOffsets) {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("partitionsWithNoSavedOffset", partitionsWithoutSavedOffsets.format());
    details.put("sourceOffsets", new TreeMap<>(transformValues(sourceOffsets, SourceOffset::toString)));
    logMilestone(Milestone.SOURCE_OFFSETS_READ, details);
  }

  public void logMissingSourceOffsetsSetToNow(PartitionSet partitionsSetToNow) {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("partitionsSetToNow", partitionsSetToNow.format());
    logMilestone(Milestone.MISSING_SOURCE_OFFSETS_SET_TO_NOW, details);
  }

  public void logTaskStopped() {
    logMilestone(Milestone.TASK_STOPPED, emptyMap());
  }

  public void logTaskCleanupComplete() {
    logMilestone(Milestone.TASK_CLEANUP_COMPLETE, emptyMap());
  }

  private void logMilestone(SourceTaskLifecycle.Milestone milestone, Map<String, Object> milestoneDetails) {
    if (enabled()) {
      LinkedHashMap<String, Object> message = new LinkedHashMap<>();
      message.put("milestone", milestone);
      message.put("taskUuid", uuid);
      getConnectorContextFromLoggingContext().ifPresent(it -> message.put("context", it));
      message.putAll(milestoneDetails);
      doLog(milestone.logLevel, message);
    }
  }

  private void doLog(LogLevel level, Object message) {
    try {
      level.log(log, "{}", Mapper.encodeAsString(message));
    } catch (Exception e) {
      level.log(log, "{}", message);
    }
  }

  private boolean enabled() {
    return true;
  }

}
