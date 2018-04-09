/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.connect.kafka.util.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.couchbase.client.core.lang.backport.java.util.Objects.requireNonNull;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG;

public enum Password {
    CONNECTION("KAFKA_COUCHBASE_PASSWORD", CONNECTION_PASSWORD_CONFIG),
    SSL_KEYSTORE("KAFKA_COUCHBASE_SSL_KEYSTORE_PASSWORD", CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG);

    private static final Logger LOGGER = LoggerFactory.getLogger(Password.class);

    private final String environmentVariableName;
    private final String configPropertyName;

    Password(String environmentVariableName, String configPropertyName) {
        this.environmentVariableName = requireNonNull(environmentVariableName);
        this.configPropertyName = requireNonNull(configPropertyName);
    }

    public String getEnvironmentVariableName() {
        return environmentVariableName;
    }

    public String getConfigPropertyName() {
        return configPropertyName;
    }

    public String get(AbstractConfig config) {
        requireNonNull(config);

        String envar = System.getenv(environmentVariableName);
        if (envar != null && !envar.isEmpty()) {
            LOGGER.debug("Using {} password from environment variable {}", this, environmentVariableName);
            return envar;
        }

        LOGGER.debug("Using {} password from config property {}", this, configPropertyName);
        return config.getPassword(configPropertyName).value();
    }
}
