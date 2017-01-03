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

import com.couchbase.connect.kafka.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CouchbaseSinkConnector extends SinkConnector {
    private Map<String, String> configProperties;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        configProperties = properties;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CouchbaseSinkConnectorConfig.config;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CouchbaseSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<Map<String, String>>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(configProperties);
        }
        return taskConfigs;
    }
}
