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
import com.couchbase.connect.kafka.util.config.annotation.Dependents;
import com.couchbase.connect.kafka.util.config.annotation.DisplayName;
import com.couchbase.connect.kafka.util.config.annotation.EnvironmentVariable;
import com.couchbase.connect.kafka.util.config.annotation.Width;
import org.apache.kafka.common.config.types.Password;

import static org.apache.kafka.common.config.ConfigDef.Width.LONG;

public interface SecurityConfig {
  /**
   * Use secure connection to Couchbase Server.
   * If true, 'couchbase.trust.store.path' and 'couchbase.trust.store.password' must also be provided.
   */
  @Dependents({"couchbase.trust.store.path", "couchbase.trust.store.password"})
  @Default("false")
  @DisplayName("Enable TLS")
  boolean enableTls();

  /**
   * Absolute filesystem path to the Java KeyStore with the CA certificate used by Couchbase Server.
   */
  @Width(LONG)
  @Default
  String trustStorePath();

  /**
   * Password to verify the integrity of the trust store.
   */
  @EnvironmentVariable("KAFKA_COUCHBASE_TRUST_STORE_PASSWORD")
  @Default
  Password trustStorePassword();
}
