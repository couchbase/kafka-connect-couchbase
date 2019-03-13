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

package com.couchbase.connect.kafka.transform;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class DeserializeJson<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    public static final String OVERVIEW_DOC =
            "Convert JSON values from a byte[] or ByteBuffer to a Map so downstream transforms can operate on the value." +
                    " Any null values are passed through unmodified." +
                    " The value schema is passed through unmodified (may be null).";

    @Override
    public R apply(R record) {
        final Object value = record.value();
        final Map newValue;

        try {
            if (value == null) {
                return record;

            } else if (value instanceof byte[]) {
                newValue = objectMapper.readValue((byte[]) value, Map.class);

            } else if (value instanceof ByteBuffer) {
                try (ByteBufferInputStream in = new ByteBufferInputStream((ByteBuffer) value)) {
                    newValue = objectMapper.readValue(in, Map.class);
                }

            } else {
                throw new DataException(getClass().getSimpleName() + " transform expected value to be a byte array or ByteBuffer but got " + value.getClass().getName());
            }

            return record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    record.valueSchema(), newValue,
                    record.timestamp());

        } catch (IOException e) {
            throw new DataException(getClass().getSimpleName() + " transform expected value to be JSON but got something else.", e);
        }
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
