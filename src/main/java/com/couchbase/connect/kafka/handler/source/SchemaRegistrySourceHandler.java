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

package com.couchbase.connect.kafka.handler.source;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceTaskConfig;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import com.couchbase.connect.kafka.util.config.SchemaFailureAction;
import com.couchbase.connect.kafka.util.config.SchemaFailureReason;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.couchbase.connect.kafka.util.SchemaHelper.buildStruct;
import static com.couchbase.connect.kafka.util.SchemaHelper.checkStruct;
import static com.couchbase.connect.kafka.util.SchemaHelper.parseSchema;

@Stability.Uncommitted
public class SchemaRegistrySourceHandler implements SourceHandler{

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegistrySourceHandler.class);
  private SchemaRegistryClient schemaRegistry;
  private String dlqTopic;
  private SchemaFailureAction schemaFailureAction;
  private String connectorName;

  @Override
  public void init(Map<String, String> configProperties) {
    schemaRegistry = SchemaRegistryClientFactory.newClient(
            Arrays.asList(configProperties.get("value.converter.schema.registry.url").split(",")),
            10,
            null,
            configProperties,
            null
    );

    CouchbaseSourceTaskConfig config = ConfigHelper.parse(CouchbaseSourceTaskConfig.class, configProperties);
    dlqTopic = config.dlqTopic();
    schemaFailureAction = config.schemaFailureAction();
    connectorName = configProperties.get("name");
  }

  @Override
  public SourceRecordBuilder handle(SourceHandlerParams params) {
    SourceRecordBuilder builder = new SourceRecordBuilder();

    builder.topic(params.topic());

    // No schema provided is shorthand for String schema
    builder.key(params.documentEvent().key());

    Schema schema = null;

    try {
      ParsedSchema parsed = schemaRegistry.getSchemas(params.topic(), false, true).get(0);
      schema = parseSchema(parsed.canonicalString());
    } catch (IOException | RestClientException e) {
      throw new RuntimeException("Failed to retrieve schemas with the following error: ", e);
    } catch (IndexOutOfBoundsException e) {
      LOGGER.debug("No schema found in Schema Registry for topic: {}", params.topic());
    }

    if (schema == null) {
      switch (schemaFailureAction) {
        case DLQ:
          LOGGER.info("No Schema found for topic: {}. Sending document with key: {} to missing schema topic: {}", params.topic(), params.documentEvent().key(), dlqTopic);
          builder.topic(dlqTopic);
          builder.value(Schema.BYTES_SCHEMA, params.documentEvent().content());
          builder.headers()
              .addString("couchbase.dlq.reason", SchemaFailureReason.SCHEMA_MISSING.name())
              .addString("couchbase.dlq.description", "No Schema found for topic: " + params.topic())
              .addString("couchbase.connector.name", connectorName)
              .addString("couchbase.destination.topic", params.topic())
              .addString("couchbase.source.document.id", params.documentEvent().qualifiedKey());
          return builder;
        case DROP:
          LOGGER.debug("No Schema found for topic: {}. Dropping document with key: {}", params.topic(), params.documentEvent().key());
          return null;
        case TERMINATE:
          throw new RuntimeException("No Schema found for topic: " + params.topic() + ". \n Register a schema or set couchbase.schema.failure.action to specify a different action to take when a schema is missing.");
        default:
          throw new RuntimeException("Unexpected schema failure action: " + schemaFailureAction);
      }
    }

    try {
      Struct record = buildStruct(schema, params.documentEvent().content());

      checkStruct(schema, record);

      builder.value(schema, record);
    } catch (Exception e) {
      switch (schemaFailureAction) {
        case DLQ:
          LOGGER.debug("Schema mismatch: ", e);
          LOGGER.info("Schema mismatch for document with key: {} Sending to mismatch topic: {}", params.documentEvent().key(), dlqTopic);
          builder.topic(dlqTopic);
          builder.value(Schema.BYTES_SCHEMA, params.documentEvent().content());
          builder.headers()
              .addString("couchbase.dlq.reason", SchemaFailureReason.SCHEMA_MISMATCH.name())
              .addString("couchbase.dlq.description", "Schema mismatch for destination topic. Version: " + schema.version())
              .addString("couchbase.connector.name", connectorName)
              .addString("couchbase.destination.topic", params.topic())
              .addString("couchbase.source.document.id", params.documentEvent().qualifiedKey());
          return builder;
        case DROP:
          LOGGER.debug("Dropping document with key: {}, due to Schema Mismatch", params.documentEvent().key());
          return null;
        case TERMINATE:
          throw new RuntimeException("Schema mismatch for document with key: " + params.documentEvent().key() + ". \n Set couchbase.schema.failure.action to specify a different action to take when there is a schema mismatch.", e);
        default:
          throw new RuntimeException("Unexpected schema failure action: " + schemaFailureAction);
      }
    }

    return builder;
  }
}
