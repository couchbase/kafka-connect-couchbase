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
import com.couchbase.client.core.env.CouchbaseThreadFactory;
import com.couchbase.client.core.util.NanoTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.couchbase.connect.kafka.util.ConnectHelper.getConnectorContextFromLoggingContext;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Stability.Internal
public final class Watchdog {
  private static final Logger log = LoggerFactory.getLogger(Watchdog.class);
  private static final ThreadFactory threadFactory = new CouchbaseThreadFactory("cb-watchdog-");

  private static class State {
    private final NanoTimestamp startTime = NanoTimestamp.now();
    private final String name;

    private State(String name) {
      this.name = requireNonNull(name);
    }
  }

  private ScheduledExecutorService executor;
  private volatile State lastObservedState = new State("stopped");
  private volatile State currentState = new State("initial");

  public void enterState(String s) {
    this.currentState = new State(s);
    log.debug("Transitioned to state: {}", s);
  }

  public synchronized void start() {
    stop();

    executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    getConnectorContextFromLoggingContext().ifPresent(ctx ->
        executor.execute(() -> Thread.currentThread().setName(Thread.currentThread().getName() + ctx))
    );

    currentState = new State("starting");
    lastObservedState = currentState;

    executor.scheduleWithFixedDelay(() -> {
      if (currentState == lastObservedState) {
        Duration elapsed = lastObservedState.startTime.elapsed();
        log.warn("Connector has been in same state ({}) for {}", lastObservedState.name, elapsed);
      }
      lastObservedState = currentState;
    }, 10, 10, SECONDS);
  }

  public synchronized void stop() {
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
  }
}
