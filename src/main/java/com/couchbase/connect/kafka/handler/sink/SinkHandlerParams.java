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

package com.couchbase.connect.kafka.handler.sink;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.CommonDurabilityOptions;
import com.couchbase.connect.kafka.util.Keyspace;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Parameter block for Sink record handling.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class SinkHandlerParams {
  private final ReactiveCluster cluster;
  private final @Nullable ReactiveCollection collection;
  private final Keyspace keyspace;
  private final SinkRecord sinkRecord;
  private final Optional<SinkDocument> document;
  private final Consumer<CommonDurabilityOptions<?>> durabilityOptions;
  private final Optional<Duration> expiry;

  /**
   * @param document (nullable)
   * @param expiry (nullable)
   */

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public SinkHandlerParams(ReactiveCluster cluster,
                           @Nullable ReactiveCollection collection,
                           Keyspace keyspace,
                           SinkRecord sinkRecord,
                           SinkDocument document,
                           Optional<Duration> expiry,
                           Consumer<CommonDurabilityOptions<?>> durabilityOptions) {
    this.cluster = requireNonNull(cluster);
    this.collection = collection;
    this.keyspace = requireNonNull(keyspace);
    this.sinkRecord = requireNonNull(sinkRecord);
    this.durabilityOptions = requireNonNull(durabilityOptions);
    this.document = Optional.ofNullable(document);
    this.expiry = requireNonNull(expiry);
  }

  /**
   * Returns a Couchbase Cluster for handlers that don't want to use
   * the suggested Collection returned by {@link #collection()}.
   * <p>
   * This is the same cluster that was passed to {@link SinkHandler#init},
   * provided here for convenience.
   */
  public ReactiveCluster cluster() {
    return cluster;
  }

  /**
   * Returns the suggested Couchbase collection to receive the message,
   * as determined by the `couchbase.topic.to.collection` and
   * `couchbase.default.collection` config properties.
   * <p>
   * A handler is free to ignore this suggestion and target a different
   * collection instead.
   */
  public ReactiveCollection collection() {
    if (collection == null) {
      throw new IllegalStateException("Can't call this method, because this SinkHandler's usesKvCollections() method returned false.");
    }
    return collection;
  }

  /**
   * Like {@link #collection()}, but returns the qualified <b>name</b>
   * of the suggested destination collection.
   */
  @Stability.Internal
  public Keyspace getKeyspace() {
    return keyspace;
  }

  /**
   * Returns the sink record received from Kafka.
   */
  public SinkRecord sinkRecord() {
    return sinkRecord;
  }

  /**
   * Returns the processed content of the sink record (possibly with
   * the document ID extracted from the message body), or an empty optional
   * if the record has a null value.
   * <p>
   * If the `couchbase.document.id` config property is specified and all
   * referenced fields are present in the document, the returned
   * document's {@link SinkDocument#id()} property will contain the document ID
   * built from the document fields. Otherwise the id() method returns an empty Optional.
   * <p>
   * If the `couchbase.remove.document.id` config property is set to `true`,
   * the fields used to generate the document ID will not appear in the returned document.
   */
  public Optional<SinkDocument> document() {
    return document;
  }

  /**
   * Applies the durability settings from the connector config to the
   * given options.
   */
  public void configureDurability(CommonDurabilityOptions<?> options) {
    durabilityOptions.accept(options);
  }

  /**
   * Returns the document expiry duration from the connector config,
   * or an empty optional if documents should not expire.
   */
  public Optional<Duration> expiry() {
    return expiry;
  }
}
