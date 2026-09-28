/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.connect.kafka.transform;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeserializeJsonTest {
  // A recognizable, synthetic stand-in for a sensitive record value.
  private static final String CANARY_VALUE = "SUPER_SECRET_CANARY_VALUE_9f3b";

  private static SinkRecord recordWithValue(Object value) {
    return new SinkRecord("topic", 0, null, null, null, value, 0L);
  }

  @Test
  public void deserializesByteBufferValue() {
    ByteBuffer buf = ByteBuffer.wrap("{\"a\":1}".getBytes(StandardCharsets.UTF_8));

    try (DeserializeJson<SinkRecord> smt = new DeserializeJson<>()) {
      SinkRecord result = smt.apply(recordWithValue(buf));
      Map<?, ?> value = (Map<?, ?>) result.value();
      assertEquals(1, value.size());
      assertEquals(1, value.get("a"));
    }
  }

  @Test
  public void parseFailureDoesNotLeakRecordValue() {
    // Not valid JSON, so Jackson fails and (by default) embeds the source bytes in the
    // parse-error location clause. The leading '@' guarantees the parse fails immediately.
    byte[] notJson = ("@@" + CANARY_VALUE + "@@").getBytes(StandardCharsets.UTF_8);

    try (DeserializeJson<SinkRecord> smt = new DeserializeJson<>()) {
      DataException thrown =
          assertThrows(DataException.class, () -> smt.apply(recordWithValue(notJson)));

      // Prove the intended (JSON parse-failure) branch actually ran.
      assertTrue(thrown.getMessage().contains("transform expected value to be JSON"),
          "expected the JSON parse-failure branch, but got: " + thrown.getMessage());

      // The record value must not appear anywhere in the exception chain (message or cause).
      for (Throwable t = thrown; t != null; t = t.getCause()) {
        String message = t.getMessage();
        assertFalse(message != null && message.contains(CANARY_VALUE),
            "record value leaked via " + t.getClass().getName() + ": " + message);
      }
    }
  }
}
