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


import com.couchbase.connect.kafka.util.Cluster;
import com.couchbase.connect.kafka.util.Config;
import com.couchbase.connect.kafka.util.Schemas;
import com.couchbase.connect.kafka.util.StringUtils;
import com.couchbase.connect.kafka.util.Version;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import org.apache.avro.Schema;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CouchbaseSourceConnector extends SourceConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceConnector.class);
    private Map<String, String> configProperties;
    private CouchbaseSourceConnectorConfig config;
    private Config bucketConfig;
    private String customSchemaString;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        configProperties = properties;
        config = new CouchbaseSourceConnectorConfig(configProperties);
        String bucket = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_CONFIG);
        String password = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG);
        List<String> clusterAddress = config.getListWorkaround(CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_CONFIG);
        bucketConfig = Cluster.fetchBucketConfig(bucket, password, clusterAddress);
        if (bucketConfig == null) {
            throw new ConnectException("Cannot fetch configuration for bucket " + bucket);
        }
        String schemaUri = config.getString(CouchbaseSourceConnectorConfig.SCHEMA_URI_CONFIG);
        if (schemaUri != null) {
            String schemaString = Schemas.fetchSchemaString(schemaUri);
            if (schemaString == null) {
                LOGGER.warn("Unable to fetch schema {}, falling back to default schema", schemaUri);
            }
            Schema defaultSchema = new AvroData(1).fromConnectSchema(Schemas.VALUE_DEFAULT_SCHEMA);
            Schema schema = new Schema.Parser().parse(schemaString);
            if (!AvroCompatibilityLevel.BACKWARD.compatibilityChecker.isCompatible(schema, defaultSchema)) {
                LOGGER.warn("Specified schema {} is not backward compatible with default, falling back to default schema");
            }
            customSchemaString = schemaString;
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CouchbaseSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<List<String>> partitionsGrouped = bucketConfig.groupGreedyToString(maxTasks);
        List<Map<String, String>> taskConfigs = new ArrayList<Map<String, String>>(partitionsGrouped.size());
        for (List<String> taskPartitions : partitionsGrouped) {
            Map<String, String> taskProps = new HashMap<String, String>(configProperties);
            taskProps.put(CouchbaseSourceTaskConfig.PARTITIONS_CONFIG,
                    StringUtils.join(taskPartitions, ","));
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }


    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CouchbaseSourceConnectorConfig.config;
    }
}
