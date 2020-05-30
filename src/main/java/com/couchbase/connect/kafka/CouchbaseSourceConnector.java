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


import com.couchbase.client.core.utils.NetworkAddress;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceConfig;
import com.couchbase.connect.kafka.util.Cluster;
import com.couchbase.connect.kafka.util.Config;
import com.couchbase.connect.kafka.util.Version;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CouchbaseSourceConnector extends SourceConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceConnector.class);
  private Map<String, String> configProperties;
  private Config bucketConfig;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    try {
      configProperties = properties;
      CouchbaseSourceConfig config = ConfigHelper.parse(CouchbaseSourceConfig.class, properties);

      setForceIpv4(config.forceIPv4());

      bucketConfig = Cluster.fetchBucketConfig(config);
      if (bucketConfig == null) {
        String bucket = config.bucket();
        throw new ConnectException("Cannot fetch configuration for bucket " + bucket);
      }
    } catch (ConfigException e) {
      throw new ConnectException("Cannot start CouchbaseSourceConnector due to configuration error", e);
    }
  }

  static void setForceIpv4(boolean forceIpv4) {
    // Can't use constant NetworkAddress.FORCE_IPV4_PROPERTY because that would trigger static init
    System.setProperty("com.couchbase.forceIPv4", String.valueOf(forceIpv4));
    if (NetworkAddress.FORCE_IPV4 != forceIpv4) {
      throw new IllegalStateException("Too late to set 'com.couchbase.forceIPv4' system property; static init for NetworkAddress already done.");
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CouchbaseSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<List<String>> partitionsGrouped = bucketConfig.groupGreedyToString(maxTasks);
    List<Map<String, String>> taskConfigs = new ArrayList<>(partitionsGrouped.size());
    for (List<String> taskPartitions : partitionsGrouped) {
      Map<String, String> taskProps = new HashMap<>(configProperties);
      // property name matches CouchbaseSourceTaskConfig.partitions()
      taskProps.put("couchbase.partitions", String.join(",", taskPartitions));
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return ConfigHelper.define(CouchbaseSourceConfig.class);
  }
}
