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

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static java.util.Objects.requireNonNull;

/**
 * Holds a reactive publisher ({@link Mono} or {@link Flux})
 * representing some completable action, and a concurrency hint
 * to ensure actions affecting the same document(s) are
 * never executed concurrently.
 */
public class SinkAction {
  private static final SinkAction IGNORE = new SinkAction(Mono.empty(), ConcurrencyHint.alwaysConcurrent());

  private final Mono<Void> action;
  private final ConcurrencyHint concurrencyHint;

  /**
   * Returns a "no-op" action that ignores the message.
   */
  public static SinkAction ignore() {
    return IGNORE;
  }

  /**
   * Returns an action that removes the document from Couchbase if it exists.
   * Uses the document ID as the concurrency hint.
   */
  public static SinkAction remove(SinkHandlerParams params, ReactiveCollection collection, String documentId) {
    RemoveOptions options = removeOptions();
    params.configureDurability(options);
    Mono<?> action = collection
        .remove(documentId, options)
        .onErrorResume(DocumentNotFoundException.class, throwable -> Mono.empty());
    return new SinkAction(action, ConcurrencyHint.of(documentId));
  }

  /**
   * Returns an action that upserts a JSON document to Couchbase.
   * Uses the document ID as the concurrency hint.
   *
   * @param params conveys expiry and durability settings
   * @param collection the collection where the document should be upserted.
   * @param documentId ID to use for the Couchbase document
   * @param json document content to upsert
   */
  public static SinkAction upsertJson(SinkHandlerParams params, ReactiveCollection collection, String documentId, byte[] json) {
    UpsertOptions options = upsertOptions()
        .transcoder(RawJsonTranscoder.INSTANCE);
    params.configureDurability(options);
    params.expiry().ifPresent(options::expiry);

    Mono<?> action = collection.upsert(documentId, json, options);
    return new SinkAction(action, ConcurrencyHint.of(documentId));
  }

  /**
   * Creates a custom sink action.
   *
   * @param action A cold publisher (Mono or Flux) encapsulating the work to be performed.
   * Any values emitted by the publisher are ignored; only the completion signal is used.
   * @param concurrencyHint Determines which actions may be performed concurrently.
   * Typically holds the ID of the Couchbase document modified by the action
   * (or {@link ConcurrencyHint#neverConcurrent()} if the affected documents are not known
   * in advance). This allows different documents to be updated concurrently, while ensuring
   * updates to the same document are executed sequentially.
   */
  @SuppressWarnings("unchecked")
  public SinkAction(Publisher<?> action, ConcurrencyHint concurrencyHint) {
    this.action = (Mono<Void>) Mono.ignoreElements(action);
    this.concurrencyHint = requireNonNull(concurrencyHint);
  }

  public Mono<Void> action() {
    return action;
  }

  public ConcurrencyHint concurrencyHint() {
    return concurrencyHint;
  }

}
