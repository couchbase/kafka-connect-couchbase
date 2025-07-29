/*
 * Copyright 2023 Couchbase, Inc.
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

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.connect.kafka.handler.sink.AnalyticsSinkHandler.StatementAndArgs;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;


public class AnalyticsSinkHandlerTest {
  @Test
  public void testDeleteQuery() {

    String expectedDeleteStatement = "DELETE FROM bucket.scope.collection WHERE `UserID`=? AND `Name`=?;";
    JsonArray expectedPositionalParameters = JsonArray.from(Arrays.asList(27, "Jinesh"));
    StatementAndArgs obtainedDeleteQuery = AnalyticsSinkHandler.deleteQuery(
        "bucket.scope.collection", JsonObject.fromJson("{\"UserID\":27,\"Name\":\"Jinesh\"}"));

    assertEquals(expectedDeleteStatement, obtainedDeleteQuery.statement());
    assertEquals(expectedPositionalParameters, obtainedDeleteQuery.args());

    expectedDeleteStatement = "DELETE FROM bucket.scope.collection WHERE `array with spaces`=?;";
    expectedPositionalParameters = JsonArray.from(Collections.singletonList(Arrays.asList("a", "b")));
    obtainedDeleteQuery = AnalyticsSinkHandler.deleteQuery(
        "bucket.scope.collection", JsonObject.fromJson("{\"array with spaces\":[\"a\",\"b\"]}"));

    assertEquals(expectedDeleteStatement, obtainedDeleteQuery.statement());
    assertEquals(expectedPositionalParameters, obtainedDeleteQuery.args());

  }

  @Test
  public void testGetJsonObject() {
    assertNull(AnalyticsSinkHandler.getJsonObject("invalid JSON"));
    assertNotNull(AnalyticsSinkHandler.getJsonObject("{\"UserID\":27,\"Name\":\"Jinesh\"}"));
  }
}
