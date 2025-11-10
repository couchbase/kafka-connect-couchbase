/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.connect.kafka.config.source;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.connect.kafka.CouchbaseSourceTask;
import com.couchbase.connect.kafka.StreamFrom;
import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.CouchbaseHeaderSetter;
import com.couchbase.connect.kafka.util.TopicMap;
import com.couchbase.connect.kafka.util.config.Contextual;
import com.couchbase.connect.kafka.util.config.annotation.ContextDocumentation;
import com.couchbase.connect.kafka.util.config.annotation.Default;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;

import static com.couchbase.connect.kafka.util.config.ConfigHelper.validate;

public interface SchemaConfig {
  /**
   * The schema to apply to the value of each record.
   * <p>
   * This property will be ignored unless the source handler specified by `couchbase.source.handler` supports it.
   * <p>
   * The built-in `com.couchbase.connect.kafka.handler.source.ConfigurableSchemaSourceHandler` supports this property.
   * ConfigurableSchemaSourceHandler expects the schema to be an Avro Schema Record, stored in a JSON object. Reference: https://avro.apache.org/docs/1.12.0/specification/
   * <p>
   * ConfigurableSchemaSourceHandler will filter out documents that do not have a JSON Object at the root, and any that do not match the specified Schema..
   */
  @Stability.Uncommitted
  @Default
  @ContextDocumentation(
      contextDescription = "the name of the collection to apply the schema to, qualified by scope",
      sampleContext = "myScope.myCollection",
      sampleValue = "{\"type\": \"typeName\", ...attributes...}"
  )
  Contextual<String> valueSchema();

  /**
   * This property will be ignored unless the source handler specified by `couchbase.source.handler` supports it.
   * The built-in 'com.couchbase.connect.kafka.handler.source.SchemaRegistrySourceHandler' supports this property.
   * <p>
   * If set to a non-empty string, Documents that do not match the registered schema for a topic will be sent to the specified schema.
   * <p>
   * If left empty, Documents that do not match the registered schema for a topic will cause the Connector Task to fail.
   */
  @Stability.Uncommitted
  @Default
  String schemaMismatchTopic();

  /**
   * This property will be ignored unless the source handler specified by `couchbase.source.handler` supports it.
   * The built-in 'com.couchbase.connect.kafka.handler.source.SchemaRegistrySourceHandler' supports this property.
   * <p>
   * If set to a non-empty string, Documents sent to a topic with no registered schema will be sent to the specified missing schema topic
   * <p>
   * If left empty, Documents sent to a topic with no registered schema will cause the Connector Task to fail.
   */
  @Stability.Uncommitted
  @Default
  String missingSchemaTopic();
}
