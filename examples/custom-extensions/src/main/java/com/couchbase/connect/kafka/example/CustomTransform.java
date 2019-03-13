/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.connect.kafka.example;

import com.couchbase.connect.kafka.util.config.EnumValidator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.singletonMap;

/**
 * A custom Single Message Transform that converts the message to schemaless JSON and transforms
 * all textual fields, either reversing them or converting them to upper case depending
 * on how the transform is configured.
 * <p>
 * To use this transform, build this project and move the resulting JAR to
 * the same location as the kafka-connect-couchbase JAR.
 * <p>
 * Here's a sample connector configuration snippet that applies the transformation twice
 * with different configurations:
 * <pre>
 * key.converter=org.apache.kafka.connect.storage.StringConverter
 * value.converter=org.apache.kafka.connect.json.JsonConverter
 * value.converter.schemas.enable=false
 *
 * transforms=reverse,lowercase
 *
 * transforms.reverse.type=com.couchbase.connect.kafka.example.CustomTransform
 * transforms.reverse.op=REVERSE
 *
 * transforms.lowercase.type=com.couchbase.connect.kafka.example.CustomTransform
 * transforms.lowercase.op=LOWER_CASE
 * </pre>
 * Try this in conjunction with the "json-producer" example code to see the airport
 * codes reversed and converted to lower case.
 */
public class CustomTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final JsonConverter jsonConverter = newSchemalessJsonConverter();

    private static JsonConverter newSchemalessJsonConverter() {
        JsonConverter converter = new JsonConverter();
        converter.configure(singletonMap("schemas.enable", false), false);
        return converter;
    }

    private static final String OP_CONFIG = "op";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(OP_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    new EnumValidator(Operation.class),
                    ConfigDef.Importance.HIGH,
                    "The type of conversion to apply to all text fields in the document. Valid values are: " + Arrays.toString(Operation.values()));

    private enum Operation implements Function<String, String> {
        REVERSE() {
            @Override
            public String apply(String s) {
                return new StringBuffer(s).reverse().toString();
            }
        },
        LOWER_CASE() {
            @Override
            public String apply(String s) {
                return s.toLowerCase(Locale.ROOT);
            }
        }
    }

    private Function<String, String> textTransformer;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final AbstractConfig config = new AbstractConfig(config(), configs);
        this.textTransformer = Operation.valueOf(config.getString(OP_CONFIG));
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        try {
            final JsonNode document = getValueAsJsonNode(record);

            // Working with a Map instead of a JsonNode might be more efficient, since you
            // won't have to convert back to a Map when submitting the result.
            // To go down that road, call getValueAsMap(record) instead.

            // Just for a fun example, this code modifies the values of all text fields.
            // For example, if the operation is REVERSE, then {"foo":"bar"} becomes {"foo":"rab"}.
            transformTextFields(document, textTransformer);

            final Map<String, Object> resultAsMap = objectMapper.convertValue(document, new TypeReference<Map<String, Object>>() {
            });

            return record.newRecord(
                    record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    null, resultAsMap,
                    record.timestamp());

        } catch (IOException e) {
            throw new DataException("Expected JSON Object but got something else.", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getValueAsMap(ConnectRecord record) throws IOException {
        return record.value() instanceof Map
                ? (Map) record.value()
                : objectMapper.convertValue(getValueAsJsonNode(record), Map.class);
    }

    private static JsonNode getValueAsJsonNode(ConnectRecord record) throws IOException {
        if (record.value() instanceof Map) {
            return objectMapper.convertValue(record.value(), JsonNode.class);
        }

        return objectMapper.readTree(getValueAsJsonBytes(record));
    }

    private static byte[] getValueAsJsonBytes(ConnectRecord record) {
        return jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    }

    // Walk the JSON tree and modify textual fields in-place.
    private void transformTextFields(JsonNode node, Function<String, String> transformer) {
        if (node.isObject()) {
            // take a snapshot of the field names to avoid concurrent modification
            final List<String> fieldNames = new ArrayList<>(node.size());
            node.fieldNames().forEachRemaining(fieldNames::add);

            for (String fieldName : fieldNames) {
                final JsonNode child = node.get(fieldName);
                if (child.isContainerNode()) {
                    transformTextFields(child, transformer);
                } else if (child.isTextual()) {
                    final String oldValue = child.textValue();
                    ((ObjectNode) node).set(fieldName, new TextNode(transformer.apply(oldValue)));
                }
            }
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                final JsonNode child = node.get(i);
                if (child.isContainerNode()) {
                    transformTextFields(child, transformer);
                } else if (child.isTextual()) {
                    final String oldValue = child.textValue();
                    ((ArrayNode) node).set(i, new TextNode(transformer.apply(oldValue)));
                }
            }
        }
    }

    @Override
    public void close() {
    }
}
