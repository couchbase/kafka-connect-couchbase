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
package com.couchbase.connect.kafka;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.connect.kafka.handler.sink.ConcurrencyHint;
import com.couchbase.connect.kafka.util.AnalyticsBatchBuilder;
import com.couchbase.connect.kafka.util.N1qlData;
import com.couchbase.connect.kafka.util.N1qlData.OperationType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AnalyticsBatchBuilderTest {

  private void generateUpsertBatchedData(List<N1qlData> data, int numberOfDataToBeGenerated, int operationRepetition, int idRepetition, List<String> keyspaces, int ksRepetition, AnalyticsBatchBuilder batchBuilder) {
    int keySpacePos = 0;
    int keySpaceCnt = 0;
    for (int i = 0; i < numberOfDataToBeGenerated; i++) {
      OperationType opType = (i / operationRepetition) % 2 == 0 ? OperationType.UPSERT : OperationType.DELETE;
      String keyspace = keyspaces.get(keySpacePos);
      JsonObject object = JsonObject.create();
      object.put("_id", "i" + i % idRepetition);
      object.put("name", "name" + i);
      N1qlData entry = new N1qlData(
          keyspace,
          opType == OperationType.UPSERT ? object.toString() : generateDeleteCondition(object),
          opType,
          ConcurrencyHint.of(object.get("_id"))
      );
      data.add(entry);
      batchBuilder.add(entry);

      keySpaceCnt++;
      if (keySpaceCnt == ksRepetition) {
        keySpacePos = (keySpacePos + 1) % keyspaces.size();
        keySpaceCnt = 0;
      }
    }
  }

  private String generateDeleteCondition(JsonObject documentKeysJson) {
    String condition = documentKeysJson.getNames().stream().map(key -> {
      Object value = documentKeysJson.get(key);
      if (value instanceof Number) {
        return String.format("%s=%s", key, value);
      } else {
        return String.format("%s=\"%s\"", key, value);
      }
    }).collect(Collectors.joining(" and "));

    return " ( " + condition + " ) ";
  }

  @Test
  public void allUpsertionWithUniqueIdsInSameKeyspace() {
    List<N1qlData> data = new ArrayList<>();
    int numberOfDataToBeGenerated = 10;
    List<String> keyspaces = new ArrayList<>();
    keyspaces.add("sample_travel.inventory.hotel");

    int batchLimit = 10;

    // Actual
    AnalyticsBatchBuilder batchBuilder = new AnalyticsBatchBuilder(batchLimit);

    generateUpsertBatchedData(data, numberOfDataToBeGenerated, numberOfDataToBeGenerated, numberOfDataToBeGenerated, keyspaces, 1, batchBuilder);

    // Expected
    // Since batchLimit >= numberOfDataToBeGenerated , they will fall just one batch
    StringBuilder expectedData = new StringBuilder().append("UPSERT INTO ").append(keyspaces.get(0)).append(" ([");
    for (int i = 0; i < numberOfDataToBeGenerated; i++) {
      if (i == numberOfDataToBeGenerated - 1) {
        expectedData.append(data.get(i).getData());
      } else {
        expectedData.append(data.get(i).getData()).append(" , ");
      }
    }
    expectedData.append("])");

    assertEquals(expectedData.toString(), batchBuilder.build().get(0));


    data = new ArrayList<>();
    // When batchLimit < numberOfDataToBeGenerated
    numberOfDataToBeGenerated = 5;
    batchLimit = 2;
    batchBuilder = new AnalyticsBatchBuilder(batchLimit);
    generateUpsertBatchedData(data, numberOfDataToBeGenerated, numberOfDataToBeGenerated, numberOfDataToBeGenerated, keyspaces, 1, batchBuilder);

    List<String> expectedDataList = new ArrayList<>();
    expectedDataList.add("UPSERT INTO sample_travel.inventory.hotel ([{\"name\":\"name0\",\"_id\":\"i0\"} , {\"name\":\"name1\",\"_id\":\"i1\"}])");
    expectedDataList.add("UPSERT INTO sample_travel.inventory.hotel ([{\"name\":\"name2\",\"_id\":\"i2\"} , {\"name\":\"name3\",\"_id\":\"i3\"}])");
    expectedDataList.add("UPSERT INTO sample_travel.inventory.hotel ([{\"name\":\"name4\",\"_id\":\"i4\"}])");

    int i = 0;
    assertEquals(expectedDataList.size(), batchBuilder.build().size());
    List<String> actualDataList = batchBuilder.build();
    while (i < expectedDataList.size()) {
      assertEquals(expectedDataList.get(i).trim(), actualDataList.get(i).trim());
      i++;
    }
  }

  @Test
  public void allUpsertInDifferentKeySpaces() {
    int numberOfDataToBeGenerated = 10;
    int batchLimit = 4;

    // will change keyspace after every 3 records
    int ksRepetition = 3;
    List<String> keySpaces = new ArrayList<>();
    keySpaces.add("sample_travel.inventory.hotel");
    keySpaces.add("sample_travel.inventory.airline");
    keySpaces.add("sample_travel.inventory.route");

    AnalyticsBatchBuilder batchBuilder = new AnalyticsBatchBuilder(batchLimit);

    List<N1qlData> data = new ArrayList<>();
    generateUpsertBatchedData(data, numberOfDataToBeGenerated, numberOfDataToBeGenerated, numberOfDataToBeGenerated, keySpaces, ksRepetition, batchBuilder);

    // Even the batch limit is 3 , but since the keyspace changes,
    // records with diff keyspace should fall in next batch
    List<String> expectedCommandList = new ArrayList<>();
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.hotel ([{\"name\":\"name0\",\"_id\":\"i0\"} , {\"name\":\"name1\",\"_id\":\"i1\"} , {\"name\":\"name2\",\"_id\":\"i2\"}])");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.airline ([{\"name\":\"name3\",\"_id\":\"i3\"} , {\"name\":\"name4\",\"_id\":\"i4\"} , {\"name\":\"name5\",\"_id\":\"i5\"}])");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.route ([{\"name\":\"name6\",\"_id\":\"i6\"} , {\"name\":\"name7\",\"_id\":\"i7\"} , {\"name\":\"name8\",\"_id\":\"i8\"}])");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.hotel ([{\"name\":\"name9\",\"_id\":\"i9\"}])");

    int i = 0;
    assertEquals(expectedCommandList.size(), batchBuilder.build().size());
    List<String> actualDataList = batchBuilder.build();
    while (i < expectedCommandList.size()) {
      assertEquals(expectedCommandList.get(i).trim(), actualDataList.get(i).trim());
      i++;
    }
  }

  @Test
  public void upsertWithIdRepetitionAndKeyspaceRepetition() {
    int numberOfDataToBeGenerated = 10;
    int batchLimit = 4;

    // will repeat id after 2 records
    int idRepetition = 2;
    int ksRepetition = 3;

    List<String> keySpaces = new ArrayList<>();
    keySpaces.add("sample_travel.inventory.hotel");
    keySpaces.add("sample_travel.inventory.airline");
    keySpaces.add("sample_travel.inventory.route");

    AnalyticsBatchBuilder batchBuilder = new AnalyticsBatchBuilder(batchLimit);

    List<N1qlData> data = new ArrayList<>();
    generateUpsertBatchedData(data, numberOfDataToBeGenerated, numberOfDataToBeGenerated, idRepetition, keySpaces, ksRepetition, batchBuilder);

    // Since the id repeats after every 2 records, there will not be more than
    // 2 records in a batch
    List<String> expectedCommandList = new ArrayList<>();
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.hotel ([{\"name\":\"name0\",\"_id\":\"i0\"} , {\"name\":\"name1\",\"_id\":\"i1\"}])");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.hotel ([{\"name\":\"name2\",\"_id\":\"i0\"}])");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.airline ([{\"name\":\"name3\",\"_id\":\"i1\"} , {\"name\":\"name4\",\"_id\":\"i0\"}])");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.airline ([{\"name\":\"name5\",\"_id\":\"i1\"}])");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.route ([{\"name\":\"name6\",\"_id\":\"i0\"} , {\"name\":\"name7\",\"_id\":\"i1\"}])");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.route ([{\"name\":\"name8\",\"_id\":\"i0\"}])");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.hotel ([{\"name\":\"name9\",\"_id\":\"i1\"}])");

    int i = 0;
    assertEquals(expectedCommandList.size(), batchBuilder.build().size());
    List<String> actualDataList = batchBuilder.build();
    while (i < expectedCommandList.size()) {
      assertEquals(expectedCommandList.get(i).trim(), actualDataList.get(i).trim());
      i++;
    }
  }

  @Test
  public void mixedUpsertDeleteWithIdRepetitionAndKeyspaceRepetition() {
    int numberOfDataToBeGenerated = 10;
    int batchLimit = 6;

    int idRepetition = 4;
    int ksRepetition = 5;
    // Operation will repeat after 3 records
    int operationRepetition = 3;

    List<String> keySpaces = new ArrayList<>();
    keySpaces.add("sample_travel.inventory.hotel");
    keySpaces.add("sample_travel.inventory.airline");
    keySpaces.add("sample_travel.inventory.route");

    AnalyticsBatchBuilder batchBuilder = new AnalyticsBatchBuilder(batchLimit);

    List<N1qlData> data = new ArrayList<>();
    generateUpsertBatchedData(data, numberOfDataToBeGenerated, operationRepetition, idRepetition, keySpaces, ksRepetition, batchBuilder);

    List<String> expectedCommandList = new ArrayList<>();

    expectedCommandList.add("UPSERT INTO sample_travel.inventory.hotel ([{\"name\":\"name0\",\"_id\":\"i0\"} , {\"name\":\"name1\",\"_id\":\"i1\"} , {\"name\":\"name2\",\"_id\":\"i2\"}])");
    expectedCommandList.add("DELETE FROM sample_travel.inventory.hotel WHERE  ( name=\"name3\" and _id=\"i3\" )  OR  ( name=\"name4\" and _id=\"i0\" ) ");
    expectedCommandList.add("DELETE FROM sample_travel.inventory.airline WHERE  ( name=\"name5\" and _id=\"i1\" )");
    expectedCommandList.add("UPSERT INTO sample_travel.inventory.airline ([{\"name\":\"name6\",\"_id\":\"i2\"} , {\"name\":\"name7\",\"_id\":\"i3\"} , {\"name\":\"name8\",\"_id\":\"i0\"}])");
    expectedCommandList.add("DELETE FROM sample_travel.inventory.airline WHERE  ( name=\"name9\" and _id=\"i1\" ) ");

    int i = 0;
    assertEquals(expectedCommandList.size(), batchBuilder.build().size());
    List<String> actualCommandList = batchBuilder.build();
    while (i < expectedCommandList.size()) {
      assertEquals(expectedCommandList.get(i).trim(), actualCommandList.get(i).trim());
      i++;
    }
  }
}
