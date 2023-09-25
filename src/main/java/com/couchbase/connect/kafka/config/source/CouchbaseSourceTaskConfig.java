/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.connect.kafka.config.source;

import com.couchbase.client.dcp.util.PartitionSet;

import java.util.List;

public interface CouchbaseSourceTaskConfig extends CouchbaseSourceConfig {
  /**
   * Set of partitions for this task to watch for changes.
   * <p>
   * Parse using {@link PartitionSet#parse(String)}.
   */
  String partitions();

  /**
   * The task ID... probably. Kafka 2.3.0 and later expose the task ID
   * in the logging context, but for earlier versions we have to assume
   * the task IDs are assigned in the same order as the configs returned
   * by Connector.taskConfigs(int).
   */
  String maybeTaskId();
}
