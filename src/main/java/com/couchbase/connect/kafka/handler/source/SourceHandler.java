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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.connect.kafka.config.common.LoggingConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Primary extension point for customizing how the Source Connector publishes messages to Kafka.
 */
public interface SourceHandler {
  /**
   * Called one time when the filter is instantiated.
   *
   * @param configProperties the connector configuration.
   */
  default void init(Map<String, String> configProperties) {
  }

  /**
   * Translates a DocumentEvent into a SourceRecord for publication to a Kafka topic.
   * <p>
   * The document event is specified by the SourceHandlerParams parameter block, along with
   * other bits of info that may be useful to custom implementations.
   * <p>
   * The handler may route the message to an arbitrary topic by setting the {@code topic}
   * property of the returned builder, or may leave it null to use the default topic
   * from the connector configuration.
   * <p>
   * The handler may filter the event stream by returning {@code null} to skip this event.
   *
   * @param params A parameter block containing input to the handler,
   * most notably the {@link DocumentEvent}.
   * @return (nullable) The record to publish to Kafka, or {@code null} to skip this event.
   */
  SourceRecordBuilder handle(SourceHandlerParams params);

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
