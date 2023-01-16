/*
 * Copyright 2020 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.dcp.core.utils.DefaultObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.connect.kafka.util.ListHelper.chunks;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListHelperTest {

  @Test
  public void chunkIntoZero() {
    assertThrows(IllegalArgumentException.class, () -> chunks(listOf(1, 2, 3, 4, 5), 0));
  }

  @Test
  public void chunkIntoVarious() throws Exception {
    check(0, 1, "[[]]");
    check(0, 2, "[[],[]]");
    check(4, 2, "[[1,2],[3,4]]");
    check(3, 1, "[[1,2,3]]");
    check(3, 2, "[[1,2],[3]]");
    check(3, 3, "[[1],[2],[3]]");
    check(3, 5, "[[1],[2],[3],[],[]]");
    check(5, 3, "[[1,2],[3,4],[5]]");
  }

  private static void check(int listSize, int numChunks, String expectedJson) throws IOException {
    final List<Integer> list = IntStream.range(1, listSize + 1).boxed().collect(toList());
    List<List<Integer>> expected = DefaultObjectMapper.readValue(expectedJson, new TypeReference<List<List<Integer>>>() {
    });
    assertEquals(expected, chunks(list, numChunks));
  }
}
