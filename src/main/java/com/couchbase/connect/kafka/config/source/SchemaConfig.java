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
import com.couchbase.connect.kafka.util.config.SchemaFailureAction;
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
   * Determines the behaviour when a Schema Mismatch or Missing Schema is encountered for a document using the Schema Registry.
   * This property will be ignored unless the source handler specified by `couchbase.source.handler` supports it.
   * The built-in `com.couchbase.connect.kafka.handler.source.SchemaRegistrySourceHandler` supports this property.
   * <p>
   * TERMINATE will stop processing documents so that the user can intervene or investigate. DROP will mark the document as SKIPPED_BECAUSE_HANDLER_SAYS_IGNORE.
   * DLQ will send the document to a default topic acting as a dead letter queue, which can optionally be specified by the user.
   */
  @Stability.Uncommitted
  @Default("TERMINATE")
  SchemaFailureAction schemaFailureAction();

  /**
   * This property will be ignored unless the source handler specified by `couchbase.source.handler` supports it.
   * The built-in `com.couchbase.connect.kafka.handler.source.SchemaRegistrySourceHandler` supports this property.
   * <p>
   * If `couchbase.schema.failure.action` is set to DLQ then documents that encounter any Schema failures will be sent to this topic.
   * Headers will be added for future investigation such as the intended destination and the reason it was sent to the DLQ
   */
  @Stability.Uncommitted
  @Default("couchbase.dlq")
  String dlqTopic();
}
