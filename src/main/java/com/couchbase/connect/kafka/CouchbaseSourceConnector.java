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


import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.dcp.util.PartitionSet;
import com.couchbase.client.java.Bucket;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceConfig;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceTaskConfig;
import com.couchbase.connect.kafka.util.CouchbaseHelper;
import com.couchbase.connect.kafka.util.ListHelper;
import com.couchbase.connect.kafka.util.Version;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.couchbase.connect.kafka.util.config.ConfigHelper.keyName;
import static java.util.stream.Collectors.toList;

public class CouchbaseSourceConnector extends SourceConnector {
  private Map<String, String> configProperties;
  private int numPartitions;
  private final ConnectorLifecycle lifecycle = new ConnectorLifecycle();

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    lifecycle.logConnectorStarted(properties.get("name"));

    try {
      configProperties = properties;
      CouchbaseSourceConfig config = ConfigHelper.parse(CouchbaseSourceConfig.class, properties);

      try (KafkaCouchbaseClient client = new KafkaCouchbaseClient(config)) {
        Bucket bucket = client.bucket();
        if (bucket == null){
          throw new ConnectException("Cannot start CouchbaseSourceConnector because bucket name is not present");
        }
        numPartitions = ((CouchbaseBucketConfig) CouchbaseHelper.getConfig(bucket, config.bootstrapTimeout())).numberOfPartitions();
      }

    } catch (ConfigException e) {
      throw new ConnectException("Cannot start CouchbaseSourceConnector due to configuration error", e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CouchbaseSourceTask.class;
  }

  private List<PartitionSet> splitPartitions(int maxTasks) {
    List<Integer> partitions = IntStream.range(0, numPartitions)
        .boxed()
        .collect(toList());

    return ListHelper.chunks(partitions, maxTasks).stream()
        .filter(list -> !list.isEmpty()) // remove empty chunks (no work for task to do)
        .map(PartitionSet::from)
        .collect(toList());
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<PartitionSet> partitionsGrouped = splitPartitions(maxTasks);
    lifecycle.logPartitionsAssigned(partitionsGrouped);

    String partitionsKey = keyName(CouchbaseSourceTaskConfig.class, CouchbaseSourceTaskConfig::partitions);
    String taskIdKey = keyName(CouchbaseSourceTaskConfig.class, CouchbaseSourceTaskConfig::maybeTaskId);

    List<Map<String, String>> taskConfigs = new ArrayList<>();
    int taskId = 0;
    for (PartitionSet taskPartitions : partitionsGrouped) {
      String formattedPartitions = taskPartitions.format();

      Map<String, String> taskProps = new HashMap<>(configProperties);
      taskProps.put(partitionsKey, formattedPartitions);
      taskProps.put(taskIdKey, "maybe-" + taskId++);
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
    lifecycle.logConnectorStopped();
  }

  @Override
  public ConfigDef config() {
    return ConfigHelper.define(CouchbaseSourceConfig.class);
  }
}
