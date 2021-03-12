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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

import static com.couchbase.client.core.util.CbObjects.defaultIfNull;

/**
 * A builder for a Kafka Connect SourceRecord.
 * <p>
 * The Couchbase connector supplies the source partition and source offset;
 * all other properties of the {@link SourceRecord} may be specified by a
 * {@link SourceHandler}.
 */
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

  /**
   * Sets the record's key.
   *
   * @param key (nullable) The key to associate with the record
   * @param keySchema (nullable) the key's schema
   * @return this object to facilitate chaining multiple methods; never null
   */
  public SourceRecordBuilder key(Schema keySchema, Object key) {
    this.keySchema = keySchema;
    this.key = key;
    return this;
  }

  /**
   * Sets the record's key.
   * <p>
   * Convenience method for String keys. Shortcut for
   * <pre>
   * key(Schema.STRING_SCHEMA, key);
   * </pre>
   *
   * @param key (nullable) The key to associate with the record
   * @return this object to facilitate chaining multiple methods; never null
   */
  public SourceRecordBuilder key(String key) {
    return key(Schema.STRING_SCHEMA, key);
  }

  /**
   * Sets the record's value.
   *
   * @param value (nullable) The value to associate with the record
   * @param valueSchema (nullable) the schema for the record's value
   * @return this object to facilitate chaining multiple methods; never null
   */
  public SourceRecordBuilder value(Schema valueSchema, Object value) {
    this.valueSchema = valueSchema;
    this.value = value;
    return this;
  }

  /**
   * Sets the record's value.
   * <p>
   * Convenience method for String values. Shortcut for
   * <pre>
   * value(Schema.STRING_SCHEMA, value);
   * </pre>
   *
   * @param value (nullable) The value to associate with the record
   * @return this object to facilitate chaining multiple methods; never null
   */
  public SourceRecordBuilder value(String value) {
    return value(Schema.STRING_SCHEMA, value);
  }

  /**
   * Sets the topic the record should be published to, or null for the connector's default topic.
   *
   * @return this object to facilitate chaining multiple methods; never null
   */
  public SourceRecordBuilder topic(String topic) {
    this.topic = topic;
    return this;
  }

  /**
   * Sets the Kafka partition the record should be published to.
   *
   * @param kafkaPartition (nullable) The value to associate with the record, or null for default partition
   * @return this object to facilitate chaining multiple methods; never null
   */
  public SourceRecordBuilder kafkaPartition(Integer kafkaPartition) {
    this.kafkaPartition = kafkaPartition;
    return this;
  }

  /**
   * Sets the record's timestamp.
   *
   * @param timestamp (nullable) The timestamp to associate with the record, or null for default timestamp assignment.
   * @return this object to facilitate chaining multiple methods; never null
   */
  public SourceRecordBuilder timestamp(Long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * Returns the header container for this builder.
   * <p>
   * The returned container may be used to set record headers.
   *
   * @return The headers container; never null
   */
  public Headers headers() {
    return headers;
  }

  @Stability.Internal
  public CouchbaseSourceRecord build(DocumentChange change,
                                     Map<String, ?> sourcePartition,
                                     Map<String, ?> sourceOffset,
                                     String defaultTopic) {
    return new CouchbaseSourceRecord(
        change,
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
