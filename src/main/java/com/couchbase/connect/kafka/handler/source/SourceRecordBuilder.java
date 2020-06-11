/*
 * Copyright 2020 Couchbase, Inc.
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
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

import static com.couchbase.client.core.util.CbObjects.defaultIfNull;

public class SourceRecordBuilder {
  private String topic;
  private Integer kafkaPartition;

  private Schema keySchema;
  private Object key;

  private Schema valueSchema;
  private Object value;

  private Long timestamp;
  private final Headers headers = new ConnectHeaders();

  public SourceRecordBuilder() {
  }

  public SourceRecordBuilder key(Schema keySchema, Object key) {
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
  public SourceRecordBuilder key(String key) {
    return key(Schema.STRING_SCHEMA, key);
  }

  public SourceRecordBuilder value(Schema valueSchema, Object value) {
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
  public SourceRecordBuilder value(String value) {
    return value(Schema.STRING_SCHEMA, value);
  }

  public SourceRecordBuilder topic(String topic) {
    this.topic = topic;
    return this;
  }

  public SourceRecordBuilder kafkaPartition(Integer kafkaPartition) {
    this.kafkaPartition = kafkaPartition;
    return this;
  }

  public SourceRecordBuilder timestamp(Long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * Returns the header container for this builder.
   * <p>
   * The returned container may be used to set record headers.
   */
  public Headers headers() {
    return headers;
  }

  public SourceRecord build(Map<String, ?> sourcePartition,
                            Map<String, ?> sourceOffset,
                            String defaultTopic) {
    return new SourceRecord(
        sourcePartition,
        sourceOffset,
        defaultIfNull(topic, defaultTopic),
        kafkaPartition,
        keySchema,
        key,
        valueSchema,
        value,
        timestamp,
        headers);
  }
}
