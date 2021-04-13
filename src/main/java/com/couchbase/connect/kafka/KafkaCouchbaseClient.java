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

import com.couchbase.client.core.env.AbstractMapPropertyLoader;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoEnvironment;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.connect.kafka.config.common.CommonConfig;
import com.couchbase.connect.kafka.util.ScopeAndCollection;
import org.apache.kafka.common.config.ConfigException;

import java.io.Closeable;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.env.IoConfig.networkResolution;
import static com.couchbase.client.core.env.TimeoutConfig.connectTimeout;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static java.util.Collections.emptyMap;

public class KafkaCouchbaseClient implements Closeable {
  private final ClusterEnvironment env;
  private final Cluster cluster;
  private final Bucket bucket;

  public KafkaCouchbaseClient(CommonConfig config) {
    this(config, emptyMap());
  }

  public KafkaCouchbaseClient(CommonConfig config, Map<String, String> clusterEnvProps) {
    List<String> clusterAddress = config.seedNodes();
    String connectionString = String.join(",", clusterAddress);
    NetworkResolution networkResolution = NetworkResolution.valueOf(config.network());

    SecurityConfig.Builder securityConfig = SecurityConfig.builder()
        .enableTls(config.enableTls())
        .enableHostnameVerification(config.enableHostnameVerification());
    if (!isNullOrEmpty(config.trustStorePath())) {
      securityConfig.trustStore(Paths.get(config.trustStorePath()), config.trustStorePassword().value(), Optional.empty());
    }
    if (!isNullOrEmpty(config.trustCertificatePath())) {
      securityConfig.trustCertificate(Paths.get(config.trustCertificatePath()));
    }

    ClusterEnvironment.Builder envBuilder = ClusterEnvironment.builder()
        .securityConfig(securityConfig)
        .ioConfig(networkResolution(networkResolution))
        .timeoutConfig(connectTimeout(config.bootstrapTimeout()));

    applyCustomEnvironmentProperties(envBuilder, clusterEnvProps);

    env = envBuilder.build();

    Authenticator authenticator = isNullOrEmpty(config.clientCertificatePath())
        ? PasswordAuthenticator.create(config.username(), config.password().value())
        : CertificateAuthenticator.fromKeyStore(Paths.get(config.clientCertificatePath()), config.clientCertificatePassword().value(), Optional.empty());

    cluster = Cluster.connect(connectionString,
        clusterOptions(authenticator)
            .environment(env));

    bucket = cluster.bucket(config.bucket());
  }

  private static void applyCustomEnvironmentProperties(ClusterEnvironment.Builder envBuilder, Map<String, String> config) {
    // Begin workaround for IoEnvironment config.
    // Prior to Java client 3.1.5, IoEnvironment isn't configurable
    // via system properties. Until then, handle it manually.
    Map<String, String> envProps = new HashMap<>(config);
    IoEnvironment.Builder ioEnvBuilder = IoEnvironment.builder();
    String nativeIoEnabled = envProps.remove("ioEnvironment.enableNativeIo");
    if (nativeIoEnabled != null) {
      ioEnvBuilder.enableNativeIo(Boolean.parseBoolean(nativeIoEnabled));
    }
    String eventLoopThreadCount = envProps.remove("ioEnvironment.eventLoopThreadCount");
    if (eventLoopThreadCount != null) {
      ioEnvBuilder.eventLoopThreadCount(Integer.parseInt(eventLoopThreadCount));
    }
    envBuilder.ioEnvironment(ioEnvBuilder);
    // End workaround for IoEnvironment config.

    try {
      envBuilder.load(new AbstractMapPropertyLoader<CoreEnvironment.Builder>() {
        @Override
        protected Map<String, String> propertyMap() {
          return envProps;
        }
      });
    } catch (Exception e) {
      throw new ConfigException("Failed to apply Couchbase environment properties; " + e.getMessage());
    }
  }

  public ClusterEnvironment env() {
    return env;
  }

  public Cluster cluster() {
    return cluster;
  }

  public Bucket bucket() {
    return bucket;
  }

  public Collection collection(ScopeAndCollection scopeAndCollection) {
    return bucket()
        .scope(scopeAndCollection.getScope())
        .collection(scopeAndCollection.getCollection());
  }

  @Override
  public void close() {
    cluster.disconnect();
    env.shutdown();
  }
}
