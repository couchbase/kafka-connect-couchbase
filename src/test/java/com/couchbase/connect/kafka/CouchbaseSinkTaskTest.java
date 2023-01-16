/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.connect.kafka;

import com.couchbase.connect.kafka.handler.sink.ConcurrencyHint;
import com.couchbase.connect.kafka.handler.sink.SinkAction;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class CouchbaseSinkTaskTest {

  // inspired by https://nitschinger.at/Reactive-Barriers-with-Reactor/
  private static class ReactiveCountDownLatch {
    private final Sinks.One<Void> inner = Sinks.one();
    private final AtomicLong ready = new AtomicLong();
    private final int needed;

    public ReactiveCountDownLatch(int count) {
      if (count < 0) {
        throw new IllegalArgumentException("count must be non-negative, but got " + count);
      }
      this.needed = count;
      if (this.needed == 0) {
        inner.emitValue(null, Sinks.EmitFailureHandler.FAIL_FAST);
      }
    }

    public void countDown() {
      if (ready.incrementAndGet() >= needed) {
        inner.emitValue(null, Sinks.EmitFailureHandler.FAIL_FAST);
      }
    }

    public Mono<Void> await() {
      return inner.asMono();
    }
  }

  /**
   * Expect actions within a batch to be executed concurrently,
   * and batches to be executed sequentially.
   */
  @Test
  public void executionConcurrency() throws Exception {
    CopyOnWriteArrayList<String> results = new CopyOnWriteArrayList<>();

    List<SinkAction> actions = new ArrayList<>();
    List<String> expectedResults = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      // This is the reverse of the action submission order;
      // the latch ensures the results appear in the expected order.
      expectedResults.addAll(listOf("a", "b"));

      // The latch introduces an artificial dependency between the actions
      // to verify they are running concurrently. If they're not,
      // there's a deadlock and the test times out.
      ReactiveCountDownLatch latch = new ReactiveCountDownLatch(1);

      // The delay causes the test to fail if the executor failed
      // to wait for each batch to complete before starting the next.
      //
      // There's probably a better way to test this without relying on the
      // system clock, but I can't figure out how to get StepVerifier
      // to work in this context :-/
      Mono<?> delay = Mono.delay(Duration.ofSeconds(1));

      actions.addAll(listOf(
          new SinkAction(
              delay
                  .then(latch.await()) // wait for "a"
                  .then(Mono.fromRunnable(() -> results.add("b"))),
              ConcurrencyHint.of("b")),

          new SinkAction(
              delay
                  .then(Mono.fromRunnable(() -> results.add("a")))
                  .then(Mono.fromRunnable(latch::countDown)),
              ConcurrencyHint.of("a"))
      ));
    }

    CouchbaseSinkTask.toMono(actions)
        .block(Duration.ofSeconds(30));

    assertEquals(expectedResults, results);
  }
}
