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
import com.couchbase.connect.kafka.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.couchbase.connect.kafka.util.ConnectHelper.getTaskIdFromLoggingContext;

public class ConnectorLifecycle {

  public enum Milestone {
    CONNECTOR_STARTED,
    CONNECTOR_STOPPED,

    /**
     * The connector has created a configuration for each task, and specified
     * which Couchbase partitions each task should steam from.
     */
    PARTITIONS_ASSIGNED,
  }

  private final LogLevel logLevel = LogLevel.INFO;

  private static final Logger log = LoggerFactory.getLogger(ConnectorLifecycle.class);

  private final String uuid = UUID.randomUUID().toString();

  public void logConnectorStarted(String name) {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("connectorVersion", Version.getVersion());
    details.put("connectorName", name);
    logMilestone(Milestone.CONNECTOR_STARTED, details);
  }

  public void logPartitionsAssigned(List<PartitionSet> partitions) {
    Map<String, Object> details = new LinkedHashMap<>();
    for (int i = 0; i < partitions.size(); i++) {
      details.put("task" + i, partitions.get(i).format());
    }
    logMilestone(Milestone.PARTITIONS_ASSIGNED, details);
  }

  public void logConnectorStopped() {
    logMilestone(Milestone.CONNECTOR_STOPPED, Collections.emptyMap());
  }

  private void logMilestone(ConnectorLifecycle.Milestone milestone, Map<String, Object> milestoneDetails) {
    if (enabled()) {
      LinkedHashMap<String, Object> message = new LinkedHashMap<>();
      message.put("milestone", milestone);
      message.put("connectorUuid", uuid);
      getTaskIdFromLoggingContext().ifPresent(id -> message.put("taskId", id));
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
