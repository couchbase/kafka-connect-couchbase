/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.connect.kafka.handler.source;


import org.junit.jupiter.api.Test;

import static com.couchbase.connect.kafka.handler.source.RawJsonSourceHandler.isValidJson;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RawJsonSourceHandlerTest {

  @Test
  public void jsonValidation() {
    assertValid("true");
    assertValid("0");
    assertValid("1.0");
    assertValid("null");
    assertValid("\"foo\"");
    assertValid("{}");
    assertValid("[]");
    assertValid("{\"foo\":{}}");
    assertValid("[[],[],[]]");

    assertInvalid("");
    assertInvalid("foo");
    assertInvalid("{}{}");
    assertInvalid("[][]");
    assertInvalid("{}true");
    assertInvalid("[]true");
    assertInvalid("{}}");
    assertInvalid("[]]");
    assertInvalid("{\"foo\":true");
    assertInvalid("[1,2,3");
  }

  private static void assertValid(String input) {
    assertTrue(isValidJson(input.getBytes(UTF_8)), "should be valid: " + input);
  }

  private static void assertInvalid(String input) {
    assertFalse(isValidJson(input.getBytes(UTF_8)), "should be invalid: " + input);
  }
}
