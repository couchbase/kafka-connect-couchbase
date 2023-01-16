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

package com.couchbase.connect.kafka;

import com.couchbase.connect.kafka.handler.sink.ConcurrencyHint;
import com.couchbase.connect.kafka.util.BatchBuilder;
import org.junit.jupiter.api.Test;

import static com.couchbase.client.dcp.core.utils.CbCollections.listOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchBuilderTest {

  @Test
  public void anyKindOfHintMayBeFirst() throws Exception {
    assertEquals(
        listOf(listOf("a")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.of("foo"))
            .build()
    );

    assertEquals(
        listOf(listOf("a")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.alwaysConcurrent())
            .build()
    );

    assertEquals(
        listOf(listOf("a")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.neverConcurrent())
            .build()
    );
  }

  @Test
  public void neverConcurrentAlwaysInsertsBarrier() throws Exception {
    assertEquals(
        listOf(listOf("a"), listOf("b"), listOf("c")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.neverConcurrent())
            .add("b", ConcurrencyHint.neverConcurrent())
            .add("c", ConcurrencyHint.neverConcurrent())
            .build()
    );

    assertEquals(
        listOf(listOf("a"), listOf("b"), listOf("c")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.of("foo"))
            .add("b", ConcurrencyHint.neverConcurrent())
            .add("c", ConcurrencyHint.of("foo"))
            .build()
    );
  }

  @Test
  public void neverConcurrentOverridesAlwaysConcurrent() throws Exception {
    assertEquals(
        listOf(listOf("a"), listOf("b"), listOf("c")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.alwaysConcurrent())
            .add("b", ConcurrencyHint.neverConcurrent())
            .add("c", ConcurrencyHint.alwaysConcurrent())
            .build()
    );
  }

  @Test
  public void alwaysConcurrentBatchesWithAnything() throws Exception {
    assertEquals(
        listOf(listOf("a", "b", "c")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.alwaysConcurrent())
            .add("b", ConcurrencyHint.alwaysConcurrent())
            .add("c", ConcurrencyHint.alwaysConcurrent())
            .build()
    );

    assertEquals(
        listOf(listOf("a", "b", "c")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.of("foo"))
            .add("b", ConcurrencyHint.alwaysConcurrent())
            .add("c", ConcurrencyHint.of("bar"))
            .build()
    );

    assertEquals(
        listOf(listOf("a", "b", "c")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.alwaysConcurrent())
            .add("b", ConcurrencyHint.of("foo"))
            .add("c", ConcurrencyHint.alwaysConcurrent())
            .build()
    );
  }

  @Test
  public void duplicatesCauseBarrier() throws Exception {
    assertEquals(
        listOf(listOf("a", "b"), listOf("c")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.of("foo"))
            .add("b", ConcurrencyHint.of("bar"))
            .add("c", ConcurrencyHint.of("bar"))
            .build()
    );

    assertEquals(
        listOf(listOf("a"), listOf("b", "c")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.of("foo"))
            .add("b", ConcurrencyHint.of("foo"))
            .add("c", ConcurrencyHint.of("bar"))
            .build()
    );

    // 3 in a row turns out to be an important test, since
    // it covers a different code path than just 2 in a row.
    assertEquals(
        listOf(listOf("a"), listOf("b"), listOf("c")),
        new BatchBuilder<>()
            .add("a", ConcurrencyHint.of("foo"))
            .add("b", ConcurrencyHint.of("foo"))
            .add("c", ConcurrencyHint.of("foo"))
            .build()
    );
  }

}
