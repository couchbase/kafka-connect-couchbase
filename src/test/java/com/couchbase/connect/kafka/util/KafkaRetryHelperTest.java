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

import com.couchbase.client.core.error.CouchbaseException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class KafkaRetryHelperTest {
  private TestClock clock;
  private KafkaRetryHelper retryHelper;

  @Before
  public void setup() {
    clock = new TestClock();
    retryHelper = new KafkaRetryHelper("test", Duration.ofSeconds(5), clock);
  }

  @After
  public void after() {
    retryHelper.close();
  }

  private static class TestClock implements KafkaRetryHelper.Clock {
    public long now = Long.MAX_VALUE;

    @Override
    public long nanoTime() {
      return now;
    }

    public void advance(Duration d) {
      now += d.toNanos();
    }
  }

  // todo upgrade to JUnit 5 and remove this
  private static <T extends Throwable> T assertThrows(Class<T> c, Runnable r) {
    try {
      r.run();
      throw new AssertionError("expected " + c.getSimpleName());
    } catch (Throwable e) {
      if (c.isAssignableFrom(e.getClass())) {
        return c.cast(e);
      }
      throw new AssertionError("expected " + c.getSimpleName() + " but got " + e.getClass());
    }
  }

  private static void throwCouchbaseException() {
    throw new CouchbaseException("oops!");
  }

  private void assertRetriable() {
    assertThrows(RetriableException.class, () ->
        retryHelper.runWithRetry(KafkaRetryHelperTest::throwCouchbaseException));
  }

  private void assertNotRetriable() {
    assertThrows(CouchbaseException.class, () ->
        retryHelper.runWithRetry(KafkaRetryHelperTest::throwCouchbaseException));
  }

  private void assertSuccess() {
    AtomicBoolean b = new AtomicBoolean();
    retryHelper.runWithRetry(() -> b.set(true));
    assertTrue(b.get());
  }

  @Test
  public void eventuallyTimesOut() throws Exception {
    assertRetriable();

    clock.advance(Duration.ofSeconds(1));
    assertRetriable();

    clock.advance(Duration.ofSeconds(4));
    assertNotRetriable();
  }

  @Test
  public void successResetsRetryStartTime() throws Exception {
    assertRetriable();

    clock.advance(Duration.ofSeconds(1));
    assertRetriable();

    assertSuccess();

    clock.advance(Duration.ofSeconds(4));
    assertRetriable();

    clock.advance(Duration.ofSeconds(1));
    assertRetriable();

    clock.advance(Duration.ofSeconds(4));
    assertNotRetriable();
  }

  @Test
  public void canSucceedImmediately() throws Exception {
    assertSuccess();
    clock.advance(Duration.ofDays(1));
    assertSuccess();
    clock.advance(Duration.ofDays(1));
    assertSuccess();
  }

  @Test
  public void zeroRetryDurationMeansNoRetry() throws Exception {
    try (KafkaRetryHelper retryHelper = new KafkaRetryHelper("test", Duration.ZERO, clock)) {
      assertThrows(CouchbaseException.class, () ->
          retryHelper.runWithRetry(KafkaRetryHelperTest::throwCouchbaseException));
    }
  }
}
