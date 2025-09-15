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
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceTaskConfig;
import com.couchbase.connect.kafka.config.source.SourceBehaviorConfig;
import com.couchbase.connect.kafka.util.ScopeAndCollection;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import com.couchbase.connect.kafka.util.config.LookupTable;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This handler enforces a custom schema using the {@link org.apache.kafka.connect.json.JsonConverter}.
 * It only supports documents where the root is a JSON object. Documents that do not match the schema or are not a JSON object will be filtered out
 * The schema is provided in the format of an Avro Schema Record, stored in a Json Object
 * The schema is read from the {@link SourceBehaviorConfig#valueSchema()} configuration property
 */
@Stability.Uncommitted
public class ConfigurableSchemaSourceHandler implements SourceHandler{

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurableSchemaSourceHandler.class);
  private LookupTable<ScopeAndCollection, Schema> valueSchemas;

  @Override
  public void init(Map<String, String> configProperties) {
    CouchbaseSourceTaskConfig config = ConfigHelper.parse(CouchbaseSourceTaskConfig.class, configProperties);

    valueSchemas = config.valueSchema()
        .mapKeys(ScopeAndCollection::parse)
        .mapValues(this::parseSchema);
  }

  private Schema parseSchema(String avroSchema) {
    org.apache.avro.Schema parsedValueSchema = new org.apache.avro.Schema.Parser().parse(avroSchema);
    return resolveSchema(parsedValueSchema);
  }

  private Schema resolveSchema(org.apache.avro.Schema schema, boolean optional) {
    switch (schema.getType()) {
      case INT:
        return optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
      case LONG:
        return optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
      case BYTES:
        return optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
      case FLOAT:
        return optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA;
      case DOUBLE:
        return optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
      case STRING:
        return optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
      case BOOLEAN:
        return optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
      case MAP:
        Schema valueSchema = resolveSchema(schema.getValueType());
        SchemaBuilder map = SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchema);
        if (optional) {
          map.optional();
        }
        return map.build();
      case ARRAY:
        Schema elementSchema = resolveSchema(schema.getElementType());
        SchemaBuilder array = SchemaBuilder.array(elementSchema);
        if (optional) {
          array.optional();
        }
        return array.build();
      case UNION:
        // Assume a union of ["null", type]
        for (org.apache.avro.Schema s : schema.getTypes()) {
          if (s.getType() != org.apache.avro.Schema.Type.NULL) {
            return resolveSchema(s, true);
          }
        }
        return null;
      case RECORD:
        SchemaBuilder record = SchemaBuilder.struct().name(schema.getName());
        if (optional) {
          record.optional();
        }
        for (org.apache.avro.Schema.Field field : schema.getFields()) {
          Schema interiorFieldSchema = resolveSchema(field.schema());
          if (interiorFieldSchema != null) {
            record.field(field.name(), interiorFieldSchema);
          }
        }
        return record;
      case NULL:
        return null;
      case ENUM:
      case FIXED: // Doesn't seem to be a fixed schema in Kafka Connect
      default:
        throw new ConfigException("Unsupported Type in Schema. We do not support Enum or Fixed types");
    }
  }

  private Schema resolveSchema(org.apache.avro.Schema schema) {
    return resolveSchema(schema, false);
  }

  @Override
  public SourceRecordBuilder handle(SourceHandlerParams params) {
    SourceRecordBuilder builder = new SourceRecordBuilder();

    Schema schemaToUse = valueSchemas.get(ScopeAndCollection.from(params.documentEvent()));

    builder.topic(params.topic());

    // No schema provided is shorthand for String schema
    builder.key(params.documentEvent().key());

    try {
      Struct record = buildStruct(schemaToUse, params.documentEvent().content());

      checkStruct(schemaToUse, record);

      builder.value(schemaToUse, record);
    } catch (Exception e) {
      LOGGER.debug("Document doesn't match specified schema. Will not be pushed to Kafka", e);
      return null;
    }

    return builder;
  }

  private void checkStruct(Schema schema, Struct struct) {
    for (Field field : schema.fields()) {
      if (struct.get(field.name()) == null && !field.schema().isOptional()) {
        throw new SchemaException("Document is missing required field: " + field.name());
      }
    }
  }

  private Struct buildStruct(Schema schema, byte[] json) {
    return buildStruct(schema, JsonObject.fromJson(json));
  }

  private Struct buildStruct(Schema schema, JsonObject json) {
    Struct struct = new Struct(schema);

    json.getNames().forEach(name -> {
      if (schema.field(name).schema().type() == Schema.Type.STRUCT) {
        struct.put(name, buildStruct(schema.field(name).schema(), json.getObject(name)));
      } else if (schema.field(name).schema().type() == Schema.Type.ARRAY) {
        struct.put(name, json.getArray(name).toList());
      } else if (schema.field(name).schema().type() == Schema.Type.MAP) {
        struct.put(name, json.getObject(name).toMap());
      } else if (schema.field(name).schema().type() == Schema.Type.BYTES) { // Assume base64 encoded string for representing bytes in JSON
        struct.put(name, Base64.decodeBase64(json.getString(name)));
      } else if (schema.field(name).schema().type() == Schema.Type.FLOAT32) {
        struct.put(name, json.getNumber(name).floatValue());
      } else { // Most primitive types map correctly without special handling
        struct.put(name, json.get(name));
      }
    });

    return struct;
  }
}
