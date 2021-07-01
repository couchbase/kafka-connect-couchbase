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
   * <p>
   * If true, you must also tell the connector which certificate to trust.
   * Specify a certificate file with 'couchbase.trust.certificate.path',
   * or a Java keystore file with 'couchbase.trust.store.path' and
   * 'couchbase.trust.store.password'.
   */
  @Dependents({
      "couchbase.trust.certificate.path",
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
   */
  @Default("true")
  @DisplayName("Enable TLS Hostname Verification")
  boolean enableHostnameVerification();

  /**
   * Absolute filesystem path to the Java keystore holding the CA certificate
   * used by Couchbase Server.
   * <p>
   * If you want to use a PEM file instead of a Java keystore,
   * specify `couchbase.trust.certificate.path` instead.
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
   * Absolute filesystem path to the PEM file containing the
   * CA certificate used by Couchbase Server.
   * <p>
   * If you want to use a Java keystore instead of a PEM file,
   * specify `couchbase.trust.store.path` instead.
   */
  @Width(LONG)
  @Default
  String trustCertificatePath();

  /**
   * Absolute filesystem path to a Java keystore or PKCS12 bundle holding
   * the private key and certificate chain to use for client certificate
   * authentication (mutual TLS).
   * <p>
   * If you supply a value for this config property, the `couchbase.username`
   * and `couchbase.password` properties will be ignored.
   */
  @Width(LONG)
  @Default
  String clientCertificatePath();

  /**
   * Password for accessing the client certificate.
   */
  @EnvironmentVariable("KAFKA_COUCHBASE_CLIENT_CERTIFICATE_PASSWORD")
  @Default
  Password clientCertificatePassword();
}
