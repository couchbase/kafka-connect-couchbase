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

package com.couchbase.connect.kafka.handler.sink;

import com.couchbase.client.java.ReactiveCluster;

import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class SinkHandlerContext {
  private final ReactiveCluster cluster;
  private final Map<String, String> configProperties;

  public SinkHandlerContext(ReactiveCluster cluster, Map<String, String> configProperties) {
    this.cluster = requireNonNull(cluster);
    this.configProperties = requireNonNull(unmodifiableMap(configProperties));
  }

  public ReactiveCluster cluster() {
    return cluster;
  }

  /**
   * Returns the connector configuration properties as an unmodifiable map.
   */
  public Map<String, String> configProperties() {
    return configProperties;
  }
}
