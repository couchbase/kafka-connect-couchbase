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
import com.couchbase.connect.kafka.config.common.LoggingConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * An alternative to {@link SourceHandler} that supports generating multiple Kafka records
 * from the same Couchbase event.
 * <p>
 * <b>WARNING:</b> If your handler's {@link #convertToSourceRecords} method ever returns multiple records,
 * the Kafka Connect worker SHOULD be configured with {@code exactly.once.source.support=enabled}.
 * Otherwise, the connector can only guarantee at least one of the related messages is delivered.
 * <p>
 * Enabling {@code exactly.once.source.support} requires Apache Kafka 3.3.0 or later. For more details, see
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Exactly-Once+Support+for+Source+Connectors">
 * KIP-618: Exactly-Once Support for Source Connectors
 * </a>.
 */
@NullMarked
@Stability.Uncommitted
public interface MultiSourceHandler {
  /**
   * Called one time when the filter is instantiated.
   *
   * @param configProperties the connector configuration.
   */
  default void init(Map<String, String> configProperties) {
  }

  /**
   * Translates a DocumentEvent (and associated parameters) into zero or more records
   * for publication to Kafka.
   * <p>
   * The document event is specified by the SourceHandlerParams parameter block, along with
   * other bits of info that may be useful to custom implementations.
   * <p>
   * The handler may route a message to an arbitrary topic by setting the {@code topic}
   * property of the returned builder, or may leave it null to use the default topic
   * from the connector configuration.
   * <p>
   * The handler may filter the event stream by returning an empty list to skip this event.
   * <p>
   * <b>WARNING:</b> See the {@link MultiSourceHandler} Javadoc for an important warning
   * about how returning multiple records can affect delivery guarantees.
   *
   * @param params A parameter block containing input to the handler,
   * most notably the {@link DocumentEvent}.
   * @return The records to publish to Kafka, or an empty list to skip this event. The list must not contain nulls.
   */
  List<SourceRecordBuilder> convertToSourceRecords(SourceHandlerParams params);

  /**
   * Called whenever the Kafka Connect framework calls {@link SourceTask#commitRecord(SourceRecord, RecordMetadata)}.
   * <p>
   * Subclasses may override this method to do something special with committed record metadata
   * beyond the usual document lifecycle logging.
   * <p>
   * This method should return quickly.
   * <p>
   * The default implementation does nothing, which is always okay.
   *
   * @see LoggingConfig#logDocumentLifecycle()
   */
  @Stability.Volatile
  default void onRecordCommitted(SourceRecord record, @Nullable RecordMetadata metadata) {
  }
}
