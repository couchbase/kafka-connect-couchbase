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

import com.couchbase.client.core.annotation.Stability;
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
  @Dependents({
      "couchbase.trust.store.path",
      "couchbase.trust.store.password",
      "couchbase.enable.hostname.verification",
      "couchbase.client.certificate.path",
      "couchbase.client.certificate.password",
  })
  @Default("false")
  @DisplayName("Enable TLS")
  boolean enableTls();

  /**
   * Set this to `false` to disable TLS hostname verification for Couchbase
   * connections. Less secure, but might be required if for some reason you
   * can't issue a certificate whose Subject Alternative Names match the
   * hostname used to connect to the server. Only disable if you understand
   * the impact and can accept the risks.
   *
   * @since 4.0.4
   */
  @Default("true")
  @DisplayName("Enable TLS Hostname Verification")
  boolean enableHostnameVerification();

  /**
   * Absolute filesystem path to the Java keystore holding the CA certificate
   * used by Couchbase Server.
   */
  @Width(LONG)
  @Default
  String trustStorePath();

  /**
   * Password for accessing the trust store.
   */
  @EnvironmentVariable("KAFKA_COUCHBASE_TRUST_STORE_PASSWORD")
  @Default
  Password trustStorePassword();

  /**
   * Absolute filesystem path to a Java keystore or PKCS12 bundle holding
   * the private key and certificate chain to use for client certificate
   * authentication (mutual TLS).
   * <p>
   * If you supply a value for this config property, the `couchbase.username`
   * and `couchbase.password` properties will be ignored.
   *
   * @since 4.0.4
   */
  @Stability.Uncommitted
  @Width(LONG)
  @Default
  String clientCertificatePath();

  /**
   * Password for accessing the client certificate.
   *
   * @since 4.0.4
   */
  @Stability.Uncommitted
  @EnvironmentVariable("KAFKA_COUCHBASE_CLIENT_CERTIFICATE_PASSWORD")
  @Default
  Password clientCertificatePassword();
}
