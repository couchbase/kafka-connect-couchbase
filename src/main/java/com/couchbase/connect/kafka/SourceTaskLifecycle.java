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
import static com.couchbase.connect.kafka.util.ConnectHelper.getConnectorContextFromLoggingContext;
import static java.util.Collections.emptyMap;

public class SourceTaskLifecycle {

  public enum Milestone {
    TASK_INITIALIZED,
    TASK_STARTED,
    TASK_STOPPED,
    SOURCE_OFFSETS_READ,
    OFFSET_COMMIT_HOOK,
  }

  private final LogLevel logLevel = LogLevel.INFO;

  private static final Logger log = LoggerFactory.getLogger(SourceTaskLifecycle.class);

  private final String uuid = UUID.randomUUID().toString();


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
    details.put("sourceOffsets", new TreeMap<>(sourceOffsets));
    logMilestone(Milestone.SOURCE_OFFSETS_READ, details);
  }

  public void logTaskStopped() {
    logMilestone(Milestone.TASK_STOPPED, emptyMap());
  }

  private void logMilestone(SourceTaskLifecycle.Milestone milestone, Map<String, Object> milestoneDetails) {
    if (enabled()) {
      LinkedHashMap<String, Object> message = new LinkedHashMap<>();
      message.put("milestone", milestone);
      message.put("taskUuid", uuid);
      getConnectorContextFromLoggingContext().ifPresent(it -> message.put("context", it));
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

  private boolean enabled() {
    return true;
  }

}
