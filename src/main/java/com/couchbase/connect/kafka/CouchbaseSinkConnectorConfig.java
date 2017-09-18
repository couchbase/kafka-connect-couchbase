/*
 * Copyright (c) 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_DISPLAY;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_DOC;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_DISPLAY;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_DOC;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_DEFAULT;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_DISPLAY;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_DOC;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_ENABLED_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_ENABLED_DEFAULT;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_ENABLED_DISPLAY;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_ENABLED_DOC;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_LOCATION_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_LOCATION_DEFAULT;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_LOCATION_DISPLAY;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_LOCATION_DOC;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_PASSWORD_DEFAULT;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_PASSWORD_DISPLAY;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_PASSWORD_DOC;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_TIMEOUT_MS_DEFAULT;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_TIMEOUT_MS_DISPLAY;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_TIMEOUT_MS_DOC;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_USERNAME_CONFIG;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_USERNAME_DEFAULT;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_USERNAME_DISPLAY;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.CONNECTION_USERNAME_DOC;
import static com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig.DATABASE_GROUP;

public class CouchbaseSinkConnectorConfig extends AbstractConfig {

    public static final String DOCUMENT_ID_POINTER_CONFIG = "couchbase.document.id";
    static final String DOCUMENT_ID_POINTER_DOC = "JSON Pointer to the property to use for the Couchbase document ID (overriding the message key).";
    static final String DOCUMENT_ID_POINTER_DISPLAY = "Document ID Pointer";
    public static final String DOCUMENT_ID_POINTER_DEFAULT = "";

    public static final String REMOVE_DOCUMENT_ID_CONFIG = "couchbase.remove.document.id";
    static final String REMOVE_DOCUMENT_ID_DOC = "Whether to remove the ID identified by '" + DOCUMENT_ID_POINTER_CONFIG + "' from the document before storing in Couchbase.";
    static final String REMOVE_DOCUMENT_ID_DISPLAY = "Remove Document ID";
    public static final boolean REMOVE_DOCUMENT_ID_DEFAULT = false;

    static ConfigDef config = baseConfigDef();

    public CouchbaseSinkConnectorConfig(Map<String, String> props) {
        super(config, props);
    }

    protected CouchbaseSinkConnectorConfig(ConfigDef config, Map<String, String> props) {
        super(config, props);
    }

    private static ConfigDef baseConfigDef() {
        ConfigDef.Recommender sslDependentsRecommender =
                new CouchbaseSourceConnectorConfig.BooleanParentRecommender(CONNECTION_SSL_ENABLED_CONFIG);
        return new ConfigDef()
                .define(CONNECTION_CLUSTER_ADDRESS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.Importance.HIGH,
                        CONNECTION_CLUSTER_ADDRESS_DOC,
                        DATABASE_GROUP, 1,
                        ConfigDef.Width.LONG,
                        CONNECTION_CLUSTER_ADDRESS_DISPLAY)

                .define(CONNECTION_BUCKET_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        CONNECTION_BUCKET_DOC,
                        DATABASE_GROUP, 2,
                        ConfigDef.Width.LONG,
                        CONNECTION_BUCKET_DISPLAY)

                .define(CONNECTION_USERNAME_CONFIG,
                        ConfigDef.Type.STRING,
                        CONNECTION_USERNAME_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        CONNECTION_USERNAME_DOC,
                        DATABASE_GROUP, 3,
                        ConfigDef.Width.LONG,
                        CONNECTION_USERNAME_DISPLAY)

                .define(CONNECTION_PASSWORD_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        CONNECTION_PASSWORD_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CONNECTION_PASSWORD_DOC,
                        DATABASE_GROUP, 4,
                        ConfigDef.Width.LONG,
                        CONNECTION_PASSWORD_DISPLAY)

                .define(CONNECTION_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CONNECTION_TIMEOUT_MS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CONNECTION_TIMEOUT_MS_DOC,
                        DATABASE_GROUP, 5,
                        ConfigDef.Width.LONG,
                        CONNECTION_TIMEOUT_MS_DISPLAY)

                .define(CONNECTION_SSL_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        CONNECTION_SSL_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CONNECTION_SSL_ENABLED_DOC,
                        DATABASE_GROUP, 6,
                        ConfigDef.Width.SHORT,
                        CONNECTION_SSL_ENABLED_DISPLAY,
                        Arrays.asList(CONNECTION_SSL_KEYSTORE_LOCATION_CONFIG, CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG))

                .define(CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        CONNECTION_SSL_KEYSTORE_PASSWORD_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CONNECTION_SSL_KEYSTORE_PASSWORD_DOC,
                        DATABASE_GROUP, 7,
                        ConfigDef.Width.LONG,
                        CONNECTION_SSL_KEYSTORE_PASSWORD_DISPLAY,
                        sslDependentsRecommender)

                .define(CONNECTION_SSL_KEYSTORE_LOCATION_CONFIG,
                        ConfigDef.Type.STRING,
                        CONNECTION_SSL_KEYSTORE_LOCATION_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CONNECTION_SSL_KEYSTORE_LOCATION_DOC,
                        DATABASE_GROUP, 8,
                        ConfigDef.Width.LONG,
                        CONNECTION_SSL_KEYSTORE_LOCATION_DISPLAY,
                        sslDependentsRecommender)

                .define(DOCUMENT_ID_POINTER_CONFIG,
                        ConfigDef.Type.STRING,
                        DOCUMENT_ID_POINTER_DEFAULT,
                        ConfigDef.Importance.LOW,
                        DOCUMENT_ID_POINTER_DOC,
                        DATABASE_GROUP, 9,
                        ConfigDef.Width.LONG,
                        DOCUMENT_ID_POINTER_DISPLAY,
                        Collections.singletonList(REMOVE_DOCUMENT_ID_CONFIG))

                .define(REMOVE_DOCUMENT_ID_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        REMOVE_DOCUMENT_ID_DEFAULT,
                        ConfigDef.Importance.LOW,
                        REMOVE_DOCUMENT_ID_DOC,
                        DATABASE_GROUP, 10,
                        ConfigDef.Width.LONG,
                        REMOVE_DOCUMENT_ID_DISPLAY,
                        new ConfigDef.Recommender() {
                            @Override
                            public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
                                return Arrays.<Object>asList(true, false);
                            }

                            @Override
                            public boolean visible(String name, Map<String, Object> parsedConfig) {
                                return !((String) parsedConfig.get(DOCUMENT_ID_POINTER_CONFIG)).isEmpty();
                            }
                        });
    }

    public String getUsername() {
        String username = getString(CONNECTION_USERNAME_CONFIG);
        if (username == null || username.isEmpty()) {
            return getString(CONNECTION_BUCKET_CONFIG);
        }
        return username;
    }
}
