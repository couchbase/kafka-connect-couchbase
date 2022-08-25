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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectionMap extends HashMap<ScopeAndCollection, String> {
  private CollectionMap() {
    throw new AssertionError("not instantiable");
  }

  public static Map<String,String> parse(List<String> topicToCollection) {
    Map<String, String> result = new HashMap<>();

    for (String entry : topicToCollection) {
      String[] topicAndCollection = entry.split("=", -1);
      if (topicAndCollection.length != 2) {
        throw new IllegalArgumentException("Bad entry: '" + entry + "'. Expected exactly one equals (=) character separating collection and topic.");
      }
      String topic = topicAndCollection[1];
      String scopeAndCollection = topicAndCollection[0];
      result.put(scopeAndCollection,topic);
    }

    return result;
  }
}
