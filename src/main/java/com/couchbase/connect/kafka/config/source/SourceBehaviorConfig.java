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

package com.couchbase.connect.kafka.config.source;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.connect.kafka.StreamFrom;
import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.SourceHandler;
import com.couchbase.connect.kafka.util.TopicMap;
import com.couchbase.connect.kafka.util.config.annotation.Default;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;

import static com.couchbase.connect.kafka.util.config.ConfigHelper.validate;

public interface SourceBehaviorConfig {
  /**
   * Name of the default Kafka topic to publish data to, for collections
   * that don't have an entry in the `couchbase.collection.to.topic` map.
   * <p>
   * This is a format string that recognizes the following placeholders:
   * <p>
   * ${bucket} refers to the bucket containing the document.
   * <p>
   * ${scope} refers to the scope containing the document.
   * <p>
   * ${collection} refers to the collection containing the document.
   */
  @Default("${bucket}.${scope}.${collection}")
  String topic();

  /**
   * A map from Couchbase collection to Kafka topic.
   * <p>
   * Collection and Topic are joined by an equals sign.
   * Map entries are delimited by commas.
   * <p>
   * For example, if you want to write messages from collection
   * "scope-a.invoices" to topic "topic1", and messages from collection
   * "scope-a.widgets" to topic "topic2", you would write:
   * "scope-a.invoices=topic1,scope-a.widgets=topic2".
   * <p>
   * Defaults to an empty map. For collections not present in this map,
   * the destination topic is determined by the `couchbase.topic` config property.
   *
   * @since 4.1.8
   */
  @Default
  List<String> collectionToTopic();

  @SuppressWarnings("unused")
  static ConfigDef.Validator collectionToTopicValidator() {
    return validate(TopicMap::parseCollectionToTopic, "scope.collection=topic,...");
  }

  /**
   * The fully-qualified class name of the source handler to use.
   * The source handler determines how the Couchbase document is converted into a Kafka record.
   * <p>
   * To publish JSON messages identical to the Couchbase documents, use
   * `com.couchbase.connect.kafka.handler.source.RawJsonSourceHandler`
   * and set `value.converter` to `org.apache.kafka.connect.converters.ByteArrayConverter`.
   * <p>
   * When using a custom source handler that filters out certain messages,
   * consider also configuring `couchbase.black.hole.topic`.
   * See that property's documentation for details.
   */
  Class<? extends SourceHandler> sourceHandler();

  /**
   * The class name of the event filter to use.
   * The event filter determines whether a database change event is ignored.
   * <p>
   * As of version 4.2.4, the default filter ignores
   * events from the Couchbase `_system` scope.
   * If you are interested in those events too, set this property to
   * `com.couchbase.connect.kafka.filter.AllPassIncludingSystemFilter`.
   * <p>
   * See also `couchbase.black.hole.topic`.
   */
  @Default("com.couchbase.connect.kafka.filter.AllPassFilter")
  Class<? extends Filter> eventFilter();

  /**
   * If this property is non-blank, the connector publishes a tiny synthetic record
   * to this topic whenever the Filter or SourceHandler ignores a source event.
   * <p>
   * This lets the connector tell the Kafka Connect framework about the
   * source offset of the ignored event. Otherwise, a long sequence of ignored
   * events in a low-traffic deployment might cause the stored source offset to lag
   * too far behind the current source offset, which can lead to rollbacks to zero
   * when the connector is restarted.
   * <p>
   * After a record is published to this topic, the record is no longer important,
   * and should be deleted as soon as possible. To reduce disk usage, configure
   * this topic to use small segments and the lowest possible retention settings.
   *
   * @since 4.1.8
   */
  @Default
  String blackHoleTopic();

  /**
   * If `couchbase.stream.from` is `SAVED_OFFSET_OR_NOW`, and this property is non-blank,
   * on startup the connector publishes to the named topic one tiny synthetic record
   * for each source partition that does not yet have a saved offset.
   * <p>
   * This lets the connector initialize the missing source offsets to "now" (the current
   * state of Couchbase).
   * <p>
   * The synthetic records have a value of null, and the same key:
   * `__COUCHBASE_INITIAL_OFFSET_TOMBSTONE__a54ee32b-4a7e-4d98-aa36-45d8417e942a`.
   * <p>
   * Consumers of this topic must ignore (or tolerate) these records.
   * <p>
   * If you specify a value for `couchbase.black.hole.topic`, specify the same value here.
   *
   * @since 4.2.4
   */
  @Stability.Uncommitted
  @Default
  String initialOffsetTopic();

  /**
   * Controls maximum size of the batch for writing into topic.
   */
  @Default("2000")
  int batchSizeMax();

  /**
   * If true, Couchbase Server will omit the document content when telling the
   * connector about a change. The document key and metadata will still be present.
   * <p>
   * If you don't care about the content of changed documents, enabling
   * this option is a great way to reduce the connector's network bandwidth
   * and memory usage.
   */
  @Default("false")
  boolean noValue();

  /**
   * When true, the connector's offsets are saved under a key that
   * includes the connector name. This is redundant, since the Kafka Connect
   * framework already isolates the offsets of connectors with different names.
   * <p>
   * Set this to true only if you've previously deployed the connector
   * to production with this set to true, and you do not wish to restart streaming
   * from the beginning. Otherwise you should ignore this property.
   */
  @Deprecated
  @Default("false")
  boolean connectorNameInOffsets();

  /**
   * Controls when in the history the connector starts streaming from.
   */
  @Default("SAVED_OFFSET_OR_BEGINNING")
  StreamFrom streamFrom();

  /**
   * If you wish to stream from all collections within a scope, specify the scope name here.
   * <p>
   * If you specify neither "couchbase.scope" nor "couchbase.collections",
   * the connector will stream from all collections of all scopes in the bucket.
   * <p>
   * Requires Couchbase Server 7.0 or later.
   */
  @Default
  String scope();

  /**
   * If you wish to stream from specific collections, specify the qualified collection
   * names here, separated by commas. A qualified name is the name of the scope
   * followed by a dot (.) and then the name of the collection. For example:
   * "tenant-foo.invoices".
   * <p>
   * If you specify neither "couchbase.scope" nor "couchbase.collections",
   * the connector will stream from all collections of all scopes in the bucket.
   * <p>
   * Requires Couchbase Server 7.0 or later.
   */
  @Default
  List<String> collections();
}
