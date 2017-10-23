/*
 * Copyright 2017 Couchbase, Inc.
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

import org.apache.kafka.connect.data.Schema;

public class CouchbaseSourceRecord {
    private final String topic;
    private final Integer kafkaPartition;
    private final Schema keySchema;
    private final Object key;
    private final Schema valueSchema;
    private final Object value;
    private final Long timestamp;

    /**
     * Arguments are all nullable. If you are unconcerned about ultimate performance,
     * consider using {@link #builder()} and setting only the properties you care about.
     */
    public CouchbaseSourceRecord(String topic, Integer kafkaPartition,
                                 Schema keySchema, Object key,
                                 Schema valueSchema, Object value,
                                 Long timestamp) {
        this.topic = topic;
        this.kafkaPartition = kafkaPartition;
        this.keySchema = keySchema;
        this.key = key;
        this.valueSchema = valueSchema;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String topic() {
        return topic;
    }

    public Integer kafkaPartition() {
        return kafkaPartition;
    }

    public Object key() {
        return key;
    }

    public Schema keySchema() {
        return keySchema;
    }

    public Object value() {
        return value;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    public Long timestamp() {
        return timestamp;
    }

    /**
     * Returns a builder object for constructing a {@link CouchbaseSourceRecord}.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String topic;
        private Integer kafkaPartition;

        private Schema keySchema;
        private Object key;

        private Schema valueSchema;
        private Object value;

        private Long timestamp;

        private Builder() {
        }

        public Builder key(Schema keySchema, Object key) {
            this.keySchema = keySchema;
            this.key = key;
            return this;
        }

        /**
         * Convenience method for String keys. Shortcut for
         * <pre>
         * key(Schema.STRING_SCHEMA, key);
         * </pre>
         */
        public Builder key(String key) {
            return key(Schema.STRING_SCHEMA, key);
        }

        public Builder value(Schema valueSchema, Object value) {
            this.valueSchema = valueSchema;
            this.value = value;
            return this;
        }

        /**
         * Convenience method for String values. Shortcut for
         * <pre>
         * value(Schema.STRING_SCHEMA, value);
         * </pre>
         */
        public Builder value(String value) {
            return value(Schema.STRING_SCHEMA, value);
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder kafkaPartition(Integer kafkaPartition) {
            this.kafkaPartition = kafkaPartition;
            return this;
        }

        public Builder timestamp(Long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public String topic() {
            return topic;
        }

        public Integer kafkaPartition() {
            return kafkaPartition;
        }

        public Schema keySchema() {
            return keySchema;
        }

        public Object key() {
            return key;
        }

        public Schema valueSchema() {
            return valueSchema;
        }

        public Object value() {
            return value;
        }

        public Long timestamp() {
            return timestamp;
        }

        public CouchbaseSourceRecord build() {
            return new CouchbaseSourceRecord(
                    topic, kafkaPartition,
                    keySchema, key,
                    valueSchema, value,
                    timestamp);
        }
    }
}
