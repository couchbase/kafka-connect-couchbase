/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.connect.kafka.handler.sink;

import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Primary extension point for customizing how the Sink Connector handles messages from Kafka.
 */
public interface SinkHandler {
  /**
   * Called one time when the filter is instantiated.
   *
   * @param context provides access to the Couchbase cluster and the connector configuration.
   */
  default void init(SinkHandlerContext context) {
  }

  /**
   * Translates a Kafka Connect {@link SinkRecord} (and associated parameters)
   * into an action to perform on the record.
   *
   * @return (nullable) The action to perform. Return null or {@link SinkAction#ignore()}
   * to skip this message.
   */
  SinkAction handle(SinkHandlerParams params);

  /**
   * Translates List of Kafka Connect {@link SinkRecord} (and associated parameters)
   * into List of actions to perform.
   */
  default List<SinkAction> handleBatch(List<SinkHandlerParams> params) {
    List<SinkAction> actions = new ArrayList<>(params.size());
    for (SinkHandlerParams param : params) {
      SinkAction action = handle(param);
      if (action != null) {
        actions.add(action);
      }
    }
    return actions;
  }

  /**
   * Returns the Couchbase document ID to use for the record
   * associated with the given params.
   *
   * @implNote The default implementation first looks for a document ID
   * extracted from the message contents according to the `couchbase.document.id`
   * config property. If that fails, it derives the key from the Kafka record
   * metadata by calling {@link #getDocumentIdFromKafkaMetadata}.
   */
  default String getDocumentId(SinkHandlerParams params) {
    return params.document()
        .flatMap(SinkDocument::id)
        .orElseGet(() -> getDocumentIdFromKafkaMetadata(params.sinkRecord()));
  }

  /**
   * Returns a document ID derived from the sink record metadata.
   *
   * @implNote The default implementation first tries to coerce the
   * record's key to a String. If that fails, a synthetic key is constructed
   * from the record's topic, partition, and offset.
   */
  default String getDocumentIdFromKafkaMetadata(SinkRecord record) {
    Object key = record.key();

    if (key instanceof String
        || key instanceof Number
        || key instanceof Boolean) {
      return key.toString();
    }

    if (key instanceof byte[]) {
      return new String((byte[]) key, UTF_8);
    }

    if (key instanceof ByteBuffer) {
      return UTF_8.decode((ByteBuffer) key).toString();
    }

    return record.topic() + "/" + record.kafkaPartition() + "/" + record.kafkaOffset();
  }
}
