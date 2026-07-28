/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package com.couchbase.connect.kafka.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.io.IOException;
import java.util.Map;

/**
 * This converter mirrors the functionality of {@link org.apache.kafka.connect.json.JsonConverter} but with the Schema code stripped out where possible.
 */
public class SchemalessJsonConverter implements Converter, HeaderConverter {

  private static final JsonNodeFactory JSON_NODE_FACTORY = new JsonNodeFactory(true);
  private final ObjectMapper objectMapper = new ObjectMapper()
      .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
      .setNodeFactory(JSON_NODE_FACTORY);

  @Override
  public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
    return toConnectData(topic, value);
  }

  @Override
  public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
    return fromConnectData(topic, schema, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    try {    
      // This handles a tombstone message
      if (value == null) {
        return null;
      }
      
      JsonNode jsonNodeValue = objectMapper.convertValue(value, JsonNode.class);

      return objectMapper.writeValueAsBytes(jsonNodeValue);
    } catch (JsonProcessingException e) {
      throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    JsonNode jsonNode;

    // This handles a tombstone message
    if (value == null) {
      return SchemaAndValue.NULL;
    }

    try {
      jsonNode = objectMapper.readTree(value);
    } catch (IOException e) {
      throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
    }

    return new SchemaAndValue(null, jsonNode);
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }
  @Override
  public void configure(Map<String, ?> map) {
  }
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }
  @Override
  public void close() throws IOException {
  }
}
