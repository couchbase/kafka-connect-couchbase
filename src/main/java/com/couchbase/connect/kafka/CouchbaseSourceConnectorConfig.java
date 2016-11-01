/**
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

import com.couchbase.connect.kafka.converter.SchemaConverter;
import com.couchbase.connect.kafka.filter.AllPassFilter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CouchbaseSourceConnectorConfig extends AbstractConfig {
    public static final String CONNECTION_CLUSTER_ADDRESS_CONFIG = "connection.cluster_address";
    private static final String CONNECTION_CLUSTER_ADDRESS_DOC = "Couchbase Cluster address to listen.";
    private static final String CONNECTION_CLUSTER_ADDRESS_DISPLAY = "Couchbase Cluster Address";

    public static final String CONNECTION_BUCKET_CONFIG = "connection.bucket";
    private static final String CONNECTION_BUCKET_DOC = "Couchbase bucket name.";
    private static final String CONNECTION_BUCKET_DISPLAY = "Couchbase Bucket";

    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "Couchbase password for the bucket.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "Couchbase Password";
    private static final String CONNECTION_PASSWORD_DEFAULT = "";

    public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
    private static final String CONNECTION_TIMEOUT_MS_DOC = "Connection timeout in milliseconds.";
    private static final String CONNECTION_TIMEOUT_MS_DISPLAY = "Connection Timeout";

    public static final String TOPIC_NAME_CONFIG = "topic.name";
    private static final String TOPIC_NAME_DOC = "Name of the Kafka topic to publish data to.";
    private static final String TOPIC_NAME_DISPLAY = "Topic Name";

    public static final String USE_SNAPSHOTS_CONFIG = "use_snapshots";
    private static final String USE_SNAPSHOTS_DOC = "If true, it will only commit into Kafka when full snapshot from Couchbase received.";
    private static final String USE_SNAPSHOTS_DISPLAY = "Use snapshots";

    public static final String DATABASE_GROUP = "Database";
    public static final String CONNECTOR_GROUP = "Connector";

    public static final String DCP_MESSAGE_CONVERTER_CLASS_CONFIG = "dcp.message.converter.class";
    private static final String DCP_MESSAGE_CONVERTER_CLASS_DOC = "The class name of the message converter to use.";
    private static final String DCP_MESSAGE_CONVERTER_CLASS_DISPLAY = "Message converter";

    public static final String EVENT_FILTER_CLASS_CONFIG = "event.filter.class";
    private static final String EVENT_FILTER_CLASS_DOC = "The class name of the event filter to use.";
    private static final String EVENT_FILTER_CLASS_DISPLAY = "Event filter";

    static ConfigDef config = baseConfigDef();

    public CouchbaseSourceConnectorConfig(Map<String, String> props) {
        super(config, props);
    }

    protected CouchbaseSourceConnectorConfig(ConfigDef config, Map<String, String> props) {
        super(config, props);
    }

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(CONNECTION_CLUSTER_ADDRESS_CONFIG,
                        ConfigDef.Type.STRING,
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

                .define(CONNECTION_PASSWORD_CONFIG,
                        ConfigDef.Type.STRING,
                        CONNECTION_PASSWORD_DEFAULT,
                        ConfigDef.Importance.LOW,
                        CONNECTION_PASSWORD_DOC,
                        DATABASE_GROUP, 3,
                        ConfigDef.Width.LONG,
                        CONNECTION_PASSWORD_DISPLAY)

                .define(CONNECTION_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        10000L,
                        ConfigDef.Importance.LOW,
                        CONNECTION_TIMEOUT_MS_DOC,
                        DATABASE_GROUP, 4,
                        ConfigDef.Width.LONG,
                        CONNECTION_TIMEOUT_MS_DISPLAY)

                .define(TOPIC_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        TOPIC_NAME_DOC,
                        CONNECTOR_GROUP, 1,
                        ConfigDef.Width.LONG,
                        TOPIC_NAME_DISPLAY)

                .define(USE_SNAPSHOTS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        USE_SNAPSHOTS_DOC,
                        CONNECTOR_GROUP, 1,
                        ConfigDef.Width.LONG,
                        USE_SNAPSHOTS_DISPLAY)

                .define(DCP_MESSAGE_CONVERTER_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        SchemaConverter.class.getName(),
                        ConfigDef.Importance.LOW,
                        DCP_MESSAGE_CONVERTER_CLASS_DOC,
                        CONNECTOR_GROUP, 2,
                        ConfigDef.Width.LONG,
                        DCP_MESSAGE_CONVERTER_CLASS_DISPLAY)

                .define(EVENT_FILTER_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        AllPassFilter.class.getName(),
                        ConfigDef.Importance.LOW,
                        EVENT_FILTER_CLASS_DOC,
                        CONNECTOR_GROUP, 3,
                        ConfigDef.Width.LONG,
                        EVENT_FILTER_CLASS_DISPLAY)
                ;
    }

    public static void main(String[] args) {
        System.out.println(config.toRst());
    }

    // FIXME: remove when type handling will be fixed in Confluent Control Center
    public List<String> getListWorkaround(String key) {
        String stringValue = getString(key);
        if (stringValue.isEmpty()) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(stringValue.split("\\s*,\\s*", -1));
        }
    }
}
