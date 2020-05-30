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

package com.couchbase.connect.kafka.config.common;

import com.couchbase.connect.kafka.util.config.annotation.Default;
import com.couchbase.connect.kafka.util.config.annotation.DisplayName;
import com.couchbase.connect.kafka.util.config.annotation.EnvironmentVariable;
import com.couchbase.connect.kafka.util.config.annotation.Importance;
import com.couchbase.connect.kafka.util.config.annotation.Width;
import org.apache.kafka.common.config.types.Password;

import java.time.Duration;
import java.util.List;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Width.LONG;

public interface ConnectionConfig {
  /**
   * Addresses of Couchbase Server nodes, delimited by commas.
   * <p>
   * If a custom port is specified, it must be the KV port
   * (which is normally 11210 for insecure connections,
   * or 11207 for secure connections).
   */
  @Width(LONG)
  @Importance(HIGH)
  List<String> seedNodes();

  /**
   * Name of the Couchbase user to authenticate as.
   */
  @Importance(HIGH)
  String username();

  /**
   * Password of the Couchbase user.
   */
  @Importance(HIGH)
  @EnvironmentVariable("KAFKA_COUCHBASE_PASSWORD")
  Password password();

  /**
   * Name of the Couchbase bucket to use.
   */
  @Width(LONG)
  @Importance(HIGH)
  String bucket();

  /**
   * The network selection strategy for connecting to a Couchbase Server cluster
   * that advertises alternate addresses.
   * <p>
   * A Couchbase node running inside a container environment (like Docker or Kubernetes)
   * might be configured to advertise both its address within the container environment
   * (known as its "default" address) as well as an "external" address for use by clients
   * connecting from outside the environment.
   * <p>
   * Setting the 'couchbase.network' config property to 'default' or 'external' forces
   * the selection of the respective addresses. Setting the value to 'auto' tells the
   * connector to select whichever network contains the addresses specified in the
   * 'couchbase.seedNodes' config property.
   */
  @Default("auto")
  String network();

  /**
   * On startup, the connector will wait this long for a Couchbase connection to be established.
   * If a connection is not established before the timeout expires, the connector will terminate.
   */
  @Default("30s")
  Duration bootstrapTimeout();

  /**
   * In a network environment that supports both IPv4 and IPv6, setting this property
   * to 'true' will force the use of IPv4 when resolving Couchbase Server hostnames.
   */
  @Default("false")
  @DisplayName("Force IPv4")
  boolean forceIPv4();
}
