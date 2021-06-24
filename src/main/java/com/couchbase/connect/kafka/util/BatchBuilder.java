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

package com.couchbase.connect.kafka.util;

import com.couchbase.connect.kafka.handler.sink.ConcurrencyHint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;

/**
 * Builds a list of batches where items within a single batch
 * may be executed concurrently, but batches themselves must be
 * executed sequentially.
 * <p>
 * Batch boundaries are determined by inspecting the provided concurrency hints.
 * A resulting batch will contain no duplicate hint values (except for
 * {@link ConcurrencyHint#alwaysConcurrent()}
 * which may appear any number of times in a single batch).
 * <p>
 * Each instance of {@link ConcurrencyHint#neverConcurrent()} results in
 * a batch containing only the single item associated with that hint.
 *
 * @param <T> batch element
 */
public class BatchBuilder<T> {
  private final List<List<T>> batches = new ArrayList<>();
  private final Set<ConcurrencyHint> hintsInCurrentBatch = new HashSet<>();
  private List<T> currentBatch = null;

  /**
   * Inspects the concurrency hint to see if a new batch must be started,
   * then adds the item to the current batch.
   *
   * @param item item to add to a batch
   * @param hint determines batch boundaries
   * @return this builder
   */
  public BatchBuilder<T> add(T item, ConcurrencyHint hint) {
    if (hint == ConcurrencyHint.neverConcurrent()) {
      // Special case so we don't waste tons of space
      // if all of the items are "never concurrent"
      doAddSingleton(item);
      return this;
    }

    if (hint != ConcurrencyHint.alwaysConcurrent() && !hintsInCurrentBatch.add(hint)) {
      insertBarrier();
      hintsInCurrentBatch.add(hint);
    }

    doAdd(item);
    return this;
  }

  /**
   * Returns the list of batches of items from previous calls to {@link #add(Object, ConcurrencyHint)}.
   */
  public List<List<T>> build() {
    return batches;
  }

  private void insertBarrier() {
    currentBatch = null;
    hintsInCurrentBatch.clear();
  }

  private void doAdd(T item) {
    if (currentBatch == null) {
      currentBatch = new ArrayList<>();
      batches.add(currentBatch);
    }
    currentBatch.add(item);
  }

  private void doAddSingleton(T item) {
    batches.add(singletonList(item));
    insertBarrier();
  }

  @Override
  public String toString() {
    return "BatchBuilder{" +
        "hintsInCurrentBatch=" + hintsInCurrentBatch +
        ", batches=" + batches +
        ", currentBatch=" + currentBatch +
        '}';
  }
}
