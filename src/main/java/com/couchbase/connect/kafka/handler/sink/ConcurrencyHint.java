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

import java.util.HashSet;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Used for concurrency control. Determines which actions may
 * be performed concurrently.
 * <p>
 * If two actions have concurrency hints with equal values,
 * the first action must complete before the second is started.
 * <p>
 * Example: Using document IDs as hint values is a way to express
 * the constraint that different documents may be updated in parallel,
 * but updates to the same document must be applied sequentially.
 *
 * @see #of(Object)
 * @see #alwaysConcurrent()
 * @see #neverConcurrent()
 */
public class ConcurrencyHint {
  /**
   * Indicates the action may be performed concurrently with any other action
   * whose hint value is NOT EQUAL to this one.
   *
   * @param value the value that must be unique withing a batch of concurrent operations.
   * Must be the sort of thing you can safely insert in a {@link HashSet}.
   */
  public static ConcurrencyHint of(Object value) {
    return new ConcurrencyHint(value);
  }

  /**
   * Returns a special hint indicating the action may be performed concurrently
   * with any other action, regardless of the other action's concurrency hint.
   */
  public static ConcurrencyHint alwaysConcurrent() {
    return ALWAYS_CONCURRENT;
  }

  /**
   * Returns a special hint indicating the action must never be performed concurrently
   * with any other action, regardless of the other action's concurrency hint.
   * <p>
   * In other words, all prior actions will be completed before this action is performed,
   * and this action will be completed before any subsequent actions are performed.
   * <p>
   * Takes precedence over {@link #alwaysConcurrent()}.
   */
  public static ConcurrencyHint neverConcurrent() {
    return NEVER_CONCURRENT;
  }

  private final Object value;

  private static final ConcurrencyHint NEVER_CONCURRENT = new ConcurrencyHint(new Object()) {
    @Override
    public String toString() {
      return "ConcurrencyHint.NEVER_CONCURRENT";
    }
  };

  private static final ConcurrencyHint ALWAYS_CONCURRENT = new ConcurrencyHint(new Object()) {
    @Override
    public String toString() {
      return "ConcurrencyHint.ALWAYS_CONCURRENT";
    }
  };

  private ConcurrencyHint(Object value) {
    this.value = requireNonNull(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConcurrencyHint that = (ConcurrencyHint) o;
    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return "ConcurrencyHint{" +
        "value=" + value +
        '}';
  }
}
