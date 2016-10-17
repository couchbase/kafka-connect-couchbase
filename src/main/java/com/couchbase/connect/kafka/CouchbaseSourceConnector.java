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
import com.couchbase.connect.kafka.util.StringUtils;
import com.couchbase.connect.kafka.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CouchbaseSourceConnector extends SourceConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseSourceConnector.class);
    private Map<String, String> configProperties;
    private CouchbaseSourceConnectorConfig config;
    private int numberOfPartitions;
    private List<String> partitions;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            configProperties = properties;
            config = new CouchbaseSourceConnectorConfig(configProperties);
            String bucket = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_CONFIG);
            String password = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG);
            List<String> clusterAddress = config.getListWorkaround(CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_CONFIG);
            numberOfPartitions = Cluster.numberOfPartitions(bucket, password, clusterAddress);
            if (numberOfPartitions == 0) {
                throw new ConnectException("Cannot determine number of Couchbase partitions");
            }
            partitions = new ArrayList<String>(numberOfPartitions);
            for (int i = 0; i < numberOfPartitions; i++) {
                partitions.add(Integer.toString(i));
            }
            LOGGER.info("partitions: {}", Arrays.toString(partitions.toArray()));
        } catch (ConfigException e) {
            throw new ConnectException("Cannot start CouchbaseSourceConnector due to configuration error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CouchbaseSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        int numGroups = Math.min(numberOfPartitions, maxTasks);

        List<List<String>> partitionsGrouped = ConnectorUtils.groupPartitions(partitions, numGroups);
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
