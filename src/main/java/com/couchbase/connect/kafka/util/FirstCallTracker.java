/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka.util;

import com.couchbase.client.core.annotation.Stability;

import java.util.concurrent.atomic.AtomicBoolean;

@Stability.Internal
public final class FirstCallTracker {
  private final AtomicBoolean alreadyCalled = new AtomicBoolean(false);

  /**
   * Return false the first time this method is called, and true on all subsequent calls.
   */
  public boolean alreadyCalled() {
    return alreadyCalled.getAndSet(true);
  }
}
