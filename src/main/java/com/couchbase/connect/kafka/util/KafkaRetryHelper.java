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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.connect.kafka.config.sink.CouchbaseSinkConfig;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class KafkaRetryHelper implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(KafkaRetryHelper.class);

  // Not static, just in case the dispatch thread is shared between tasks.
  private final ThreadLocal<Deadline> deadline = new ThreadLocal<>();

  private final Duration retryTimeout;
  private final Clock clock;
  private final String actionDescription;

  @FunctionalInterface
  interface Clock {
    long nanoTime();
  }

  static class Deadline {
    private final Clock clock;
    private final long startNanos;
    private final long durationNanos;

    Deadline(Clock clock, Duration d) {
      this.clock = requireNonNull(clock);
      this.startNanos = clock.nanoTime();
      this.durationNanos = d.toNanos();
    }

    public boolean hasTimeLeft() {
      return clock.nanoTime() - startNanos < durationNanos;
    }
  }

  /**
   * @param actionDescription just for display in log messages
   * @param retryTimeout total allowed time for all retries.
   */
  public KafkaRetryHelper(String actionDescription, Duration retryTimeout) {
    this(actionDescription, retryTimeout, System::nanoTime);
  }

  KafkaRetryHelper(String actionDescription, Duration retryTimeout, Clock clock) {
    this.actionDescription = requireNonNull(actionDescription);
    this.retryTimeout = Duration.ofNanos(toNanosSaturated(retryTimeout));
    this.clock = requireNonNull(clock);

    log.info("Initialized retry helper for {} with timeout {}",
        actionDescription, this.retryTimeout);
  }

  private static long toNanosSaturated(Duration d) {
    try {
      return d.toNanos();
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  /**
   * Runs the given action. If the action throws an exception and the retry timeout has not expired,
   * propagate the exception as a `RetriableException` so the Kafka Connect framework will call the
   * "put" method again with the same arguments after a delay.
   * <p>
   * Limits retries by storing the timestamp of the first failure in a thread-local variable
   * and checking it against the retry timeout after every failed attempt.
   * <p>
   * Assumes the same thread is used for all attempts. This is typically fine, since the Kafka Connect
   * framework always calls "put" on the same thread.
   *
   * @param r the action to wrap with
   * @throws RetriableException if the runnable throws an exception and the retry timeout has not expired.
   */
  public void runWithRetry(Runnable r) {
    try {
      if (deadline.get() != null) {
        log.info("Retrying {}", actionDescription);
      }

      r.run();

      if (deadline.get() != null) {
        deadline.remove();
        log.info("Retry for {} succeeded.", actionDescription);
      }

    } catch (Exception e) {
      if (retryTimeout.isZero()) {
        String retryTimeoutName = ConfigHelper.keyName(CouchbaseSinkConfig.class, CouchbaseSinkConfig::retryTimeout);

        log.error("Initial attempt for {} failed. Retry is disabled. Connector will terminate. " +
            "To mitigate this kind of failure, enable retry by setting the '{}' connector config property.",
            actionDescription, retryTimeoutName, e);
        throw e;
      }

      if (deadline.get() == null) {
        deadline.set(new Deadline(clock, retryTimeout));
        throw new RetriableException("Initial attempt for " + actionDescription + " failed. Will try again later.", e);
      }

      if (deadline.get().hasTimeLeft()) {
        throw new RetriableException("Retry for " + actionDescription + " failed. Will try again later.", e);
      }

      log.error("Retry for {} failed. Retry timeout ({}) expired. Connector will terminate.",
          actionDescription, retryTimeout, e);
      throw e;
    }
  }

  @Override
  public void close() {
    deadline.remove();
  }
}
