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

package com.couchbase.connect.kafka;

import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.connect.kafka.config.common.CommonConfig;

import java.io.Closeable;
import java.io.File;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.env.IoConfig.networkResolution;
import static com.couchbase.client.core.env.TimeoutConfig.connectTimeout;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;

public class KafkaCouchbaseClient implements Closeable {
  private final ClusterEnvironment env;
  private final Cluster cluster;

  public KafkaCouchbaseClient(CommonConfig config) {
    List<String> clusterAddress = config.seedNodes();
    String connectionString = String.join(",", clusterAddress);
    NetworkResolution networkResolution = NetworkResolution.valueOf(config.network());

    SecurityConfig.Builder securityConfig = SecurityConfig.builder()
        .enableTls(config.enableTls());
    if (!isNullOrEmpty(config.trustStorePath())) {
      securityConfig.trustStore(new File(config.trustStorePath()).toPath(), config.trustStorePassword().value(), Optional.empty());
    }

    env = ClusterEnvironment.builder()
        .securityConfig(securityConfig)
        .ioConfig(networkResolution(networkResolution))
        .timeoutConfig(connectTimeout(config.bootstrapTimeout()))
        .build();

    cluster = Cluster.connect(connectionString,
        clusterOptions(config.username(), config.password().value())
            .environment(env));
  }

  public ClusterEnvironment env() {
    return env;
  }

  public Cluster cluster() {
    return cluster;
  }

  @Override
  public void close() {
    cluster.disconnect();
    env.shutdown();
  }
}
