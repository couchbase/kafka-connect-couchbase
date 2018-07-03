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

import com.couchbase.client.core.logging.RedactionLevel;
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.connect.kafka.filter.AllPassFilter;
import com.couchbase.connect.kafka.handler.source.DefaultSchemaSourceHandler;
import com.couchbase.connect.kafka.util.config.BooleanParentRecommender;
import com.couchbase.connect.kafka.util.config.DurationValidator;
import com.couchbase.connect.kafka.util.config.EnumRecommender;
import com.couchbase.connect.kafka.util.config.EnumValidator;
import com.couchbase.connect.kafka.util.config.Password;
import com.couchbase.connect.kafka.util.config.SizeValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

public class CouchbaseSourceConnectorConfig extends AbstractConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceConnectorConfig.class);

    public static final String DATABASE_GROUP = "Database";
    public static final String CONNECTOR_GROUP = "Connector";

    public static final String CONNECTION_CLUSTER_ADDRESS_CONFIG = "connection.cluster_address";
    static final String CONNECTION_CLUSTER_ADDRESS_DOC = "Couchbase Cluster addresses to listen (use comma to specify several).";
    static final String CONNECTION_CLUSTER_ADDRESS_DISPLAY = "Couchbase Cluster Address";

    public static final String CONNECTION_BUCKET_CONFIG = "connection.bucket";
    static final String CONNECTION_BUCKET_DOC = "Couchbase bucket name.";
    static final String CONNECTION_BUCKET_DISPLAY = "Couchbase Bucket";

    public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
    static final String CONNECTION_USERNAME_DOC = "Couchbase user name (for Couchbase Server, it might be different from bucket name).";
    static final String CONNECTION_USERNAME_DISPLAY = "Couchbase Username";
    public static final String CONNECTION_USERNAME_DEFAULT = "";

    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    static final String CONNECTION_PASSWORD_DOC = "Couchbase password for the bucket. May be overridden with the "
            + Password.CONNECTION.getEnvironmentVariableName() + " environment variable.";
    static final String CONNECTION_PASSWORD_DISPLAY = "Couchbase Password";
    public static final String CONNECTION_PASSWORD_DEFAULT = "";

    public static final String CONNECTION_SSL_ENABLED_CONFIG = "connection.ssl.enabled";
    static final String CONNECTION_SSL_ENABLED_DOC = "Use SSL to connect to Couchbase. This feature only available in Couchbase Enterprise.";
    static final String CONNECTION_SSL_ENABLED_DISPLAY = "Use SSL";
    public static final boolean CONNECTION_SSL_ENABLED_DEFAULT = false;

    public static final String CONNECTION_SSL_KEYSTORE_LOCATION_CONFIG = "connection.ssl.keystore.location";
    static final String CONNECTION_SSL_KEYSTORE_LOCATION_DOC = "The location of the key store file.";
    static final String CONNECTION_SSL_KEYSTORE_LOCATION_DISPLAY = "Keystore Location";
    public static final String CONNECTION_SSL_KEYSTORE_LOCATION_DEFAULT = "";

    public static final String CONNECTION_SSL_KEYSTORE_PASSWORD_CONFIG = "connection.ssl.keystore.password";
    static final String CONNECTION_SSL_KEYSTORE_PASSWORD_DOC = "The password of the private key in the key store file. May be overridden with the "
            + Password.SSL_KEYSTORE.getEnvironmentVariableName() + " environment variable.";
    static final String CONNECTION_SSL_KEYSTORE_PASSWORD_DISPLAY = "Keystore Password";
    public static final String CONNECTION_SSL_KEYSTORE_PASSWORD_DEFAULT = "";

    public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
    static final String CONNECTION_TIMEOUT_MS_DOC = "Connection timeout in milliseconds.";
    static final String CONNECTION_TIMEOUT_MS_DISPLAY = "Connection Timeout";
    public static final long CONNECTION_TIMEOUT_MS_DEFAULT = 10000L;

    public static final String TOPIC_NAME_CONFIG = "topic.name";
    private static final String TOPIC_NAME_DOC = "Name of the Kafka topic to publish data to.";
    private static final String TOPIC_NAME_DISPLAY = "Topic Name";

    public static final String USE_SNAPSHOTS_CONFIG = "use_snapshots";
    private static final String USE_SNAPSHOTS_DOC = "If true, it will only commit into Kafka when full snapshot from Couchbase received.";
    private static final String USE_SNAPSHOTS_DISPLAY = "Use snapshots";
    public static final boolean USE_SNAPSHOTS_DEFAULT = false;

    public static final String DCP_MESSAGE_CONVERTER_CLASS_CONFIG = "dcp.message.converter.class";
    private static final String DCP_MESSAGE_CONVERTER_CLASS_DOC = "The class name of the message converter to use.";
    private static final String DCP_MESSAGE_CONVERTER_CLASS_DISPLAY = "Message converter";

    public static final String EVENT_FILTER_CLASS_CONFIG = "event.filter.class";
    private static final String EVENT_FILTER_CLASS_DOC = "The class name of the event filter to use.";
    private static final String EVENT_FILTER_CLASS_DISPLAY = "Event filter";

    public static final String BATCH_SIZE_MAX_CONFIG = "batch.size.max";
    private static final String BATCH_SIZE_MAX_DOC = "Controls maximum size of the batch for writing into topic.";
    private static final String BATCH_SIZE_MAX_DISPLAY = "Batch size";
    public static final int BATCH_SIZE_MAX_DEFAULT = 2000;

    public static final String COMPAT_NAMES_CONFIG = "compat.connector_name_in_offsets";
    private static final String COMPAT_NAMES_DOC = "If true, the library will use name in the offsets to allow multiple connectors for the same bucket.";
    private static final String COMPAT_NAMES_DISPLAY = "Use connector name in offsets";
    public static final boolean COMPAT_NAMES_DEFAULT = false;

    public static final String STREAM_FROM_CONFIG = "couchbase.stream_from";
    private static final String STREAM_FROM_DOC = "Controls when in history then connector starts streaming from.";
    private static final String STREAM_FROM_DISPLAY = "Stream from";
    public static final String STREAM_FROM_DEFAULT = StreamFrom.SAVED_OFFSET_OR_BEGINNING.name();

    public static final String LOG_REDACTION_CONFIG = "couchbase.log_redaction";
    static final String LOG_REDACTION_DOC = "Determines which kinds of sensitive log messages from the Couchbase connector will be tagged for later redaction by the Couchbase log redaction tool. " +
            "NONE = no tagging; PARTIAL = user data is tagged; FULL = user, meta, and system data is tagged.";
    static final String LOG_REDACTION_DISPLAY = "Log redaction";
    public static final String LOG_REDACTION_DEFAULT = RedactionLevel.NONE.name();

    public static final String COMPRESSION_CONFIG = "couchbase.compression";
    static final String COMPRESSION_DOC = "To reduce bandwidth usage, Couchbase Server 5.5 and later can send documents to the connector in compressed form. " +
            "(Messages are always published to the Kafka topic in uncompressed form, regardless of this setting.)";
    static final String COMPRESSION_DISPLAY = "Compression";
    public static final String COMPRESSION_DEFAULT = CompressionMode.ENABLED.name();

    public static final String FORCE_IPV4_CONFIG = "couchbase.forceIPv4";
    static final String FORCE_IPV4_DOC = "In a network environment that supports both IPv4 and IPv6, setting this property" +
            " to 'true' will force the use of IPv4 when resolving Couchbase Server hostnames.";
    static final String FORCE_IPV4_DISPLAY = "Force hostname resolution to use IPv4";
    public static final boolean FORCE_IPV4_DEFAULT = false;

    public static final String PERSISTENCE_POLLING_INTERVAL_CONFIG = "couchbase.persistence_polling_interval";
    static final String PERSISTENCE_POLLING_INTERVAL_DOC = "How frequently to poll Couchbase Server to see which changes are ready to be published. Specify `0` to disable polling, or an integer followed by a time qualifier (example: 100ms)";
    static final String PERSISTENCE_POLLING_INTERVAL_DISPLAY = "Persistence polling interval";
    public static final String PERSISTENCE_POLLING_INTERVAL_DEFAULT = "100ms";

    public static final String FLOW_CONTROL_BUFFER_CONFIG = "couchbase.flow_control_buffer";
    static final String FLOW_CONTROL_BUFFER_DOC = "How much heap space should be reserved for the flow control buffer. Specify an integer followed by a size qualifier (example: 128m)";
    static final String FLOW_CONTROL_BUFFER_DISPLAY = "Flow control buffer size";
    public static final String FLOW_CONTROL_BUFFER_DEFAULT = "128m";

    static ConfigDef config = baseConfigDef();
    private final String connectorName;

    public CouchbaseSourceConnectorConfig(Map<String, String> props) {
        this(config, props);
    }

    protected CouchbaseSourceConnectorConfig(ConfigDef config, Map<String, String> props) {
        super(config, props);
        connectorName = props.containsKey("name") ? props.get("name") : UUID.randomUUID().toString();
    }

    public String getConnectorName() {
        return connectorName;
    }

    public static ConfigDef baseConfigDef() {
        ConfigDef.Recommender sslDependentsRecommender = new BooleanParentRecommender(CONNECTION_SSL_ENABLED_CONFIG);

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

                .define(TOPIC_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        TOPIC_NAME_DOC,
                        CONNECTOR_GROUP, 1,
                        ConfigDef.Width.LONG,
                        TOPIC_NAME_DISPLAY)

                .define(USE_SNAPSHOTS_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        USE_SNAPSHOTS_DEFAULT,
                        ConfigDef.Importance.LOW,
                        USE_SNAPSHOTS_DOC,
                        CONNECTOR_GROUP, 2,
                        ConfigDef.Width.LONG,
                        USE_SNAPSHOTS_DISPLAY)

                .define(DCP_MESSAGE_CONVERTER_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        DefaultSchemaSourceHandler.class.getName(),
                        ConfigDef.Importance.LOW,
                        DCP_MESSAGE_CONVERTER_CLASS_DOC,
                        CONNECTOR_GROUP, 3,
                        ConfigDef.Width.LONG,
                        DCP_MESSAGE_CONVERTER_CLASS_DISPLAY)

                .define(EVENT_FILTER_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        AllPassFilter.class.getName(),
                        ConfigDef.Importance.LOW,
                        EVENT_FILTER_CLASS_DOC,
                        CONNECTOR_GROUP, 4,
                        ConfigDef.Width.LONG,
                        EVENT_FILTER_CLASS_DISPLAY)

                .define(BATCH_SIZE_MAX_CONFIG,
                        ConfigDef.Type.INT,
                        BATCH_SIZE_MAX_DEFAULT,
                        ConfigDef.Importance.LOW,
                        BATCH_SIZE_MAX_DOC,
                        CONNECTOR_GROUP, 5,
                        ConfigDef.Width.LONG,
                        BATCH_SIZE_MAX_DISPLAY)

                .define(COMPAT_NAMES_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        COMPAT_NAMES_DEFAULT,
                        ConfigDef.Importance.LOW,
                        COMPAT_NAMES_DOC,
                        CONNECTOR_GROUP, 6,
                        ConfigDef.Width.LONG,
                        COMPAT_NAMES_DISPLAY)

                .define(STREAM_FROM_CONFIG,
                        ConfigDef.Type.STRING,
                        STREAM_FROM_DEFAULT,
                        new EnumValidator(StreamFrom.class),
                        ConfigDef.Importance.LOW,
                        STREAM_FROM_DOC,
                        CONNECTOR_GROUP, 7,
                        ConfigDef.Width.LONG,
                        STREAM_FROM_DISPLAY,
                        new EnumRecommender(StreamFrom.class))

                .define(LOG_REDACTION_CONFIG,
                        ConfigDef.Type.STRING,
                        LOG_REDACTION_DEFAULT,
                        new EnumValidator(RedactionLevel.class),
                        ConfigDef.Importance.LOW,
                        LOG_REDACTION_DOC,
                        CONNECTOR_GROUP, 8,
                        ConfigDef.Width.LONG,
                        LOG_REDACTION_DISPLAY,
                        new EnumRecommender(RedactionLevel.class))

                .define(COMPRESSION_CONFIG,
                        ConfigDef.Type.STRING,
                        COMPRESSION_DEFAULT,
                        new EnumValidator(CompressionMode.class),
                        ConfigDef.Importance.LOW,
                        COMPRESSION_DOC,
                        CONNECTOR_GROUP, 9,
                        ConfigDef.Width.LONG,
                        COMPRESSION_DISPLAY,
                        new EnumRecommender(CompressionMode.class))

                .define(FORCE_IPV4_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        FORCE_IPV4_DEFAULT,
                        ConfigDef.Importance.LOW,
                        FORCE_IPV4_DOC,
                        CONNECTOR_GROUP, 10,
                        ConfigDef.Width.LONG,
                        FORCE_IPV4_DISPLAY)

                .define(FLOW_CONTROL_BUFFER_CONFIG,
                        ConfigDef.Type.STRING,
                        FLOW_CONTROL_BUFFER_DEFAULT,
                        new SizeValidator(),
                        ConfigDef.Importance.LOW,
                        FLOW_CONTROL_BUFFER_DOC,
                        CONNECTOR_GROUP, 11,
                        ConfigDef.Width.LONG,
                        FLOW_CONTROL_BUFFER_DISPLAY)

                .define(PERSISTENCE_POLLING_INTERVAL_CONFIG,
                        ConfigDef.Type.STRING,
                        PERSISTENCE_POLLING_INTERVAL_DEFAULT,
                        new DurationValidator(),
                        ConfigDef.Importance.LOW,
                        PERSISTENCE_POLLING_INTERVAL_DOC,
                        CONNECTOR_GROUP, 12,
                        ConfigDef.Width.LONG,
                        PERSISTENCE_POLLING_INTERVAL_DISPLAY)
                ;
    }

    public String getUsername() {
        String username = getString(CONNECTION_USERNAME_CONFIG);
        if (username == null || username.isEmpty()) {
            return getString(CONNECTION_BUCKET_CONFIG);
        }
        return username;
    }

    public <E extends Enum<E>> E getEnum(Class<E> enumClass, String key) {
        String configValue = getString(key);
        try {
            return Enum.valueOf(enumClass, configValue);
        } catch (Exception e) {
            throw new ConfigException("Bad value '" + configValue + "' for config key '" + key + "'" +
                    "; must be one of " + Arrays.toString(enumClass.getEnumConstants()));
        }
    }

    public static void main(String[] args) {
        System.out.println(config.toRst());
    }

}
