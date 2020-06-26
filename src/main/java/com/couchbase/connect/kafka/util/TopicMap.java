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

public class TopicMap extends HashMap<String, ScopeAndCollection> {
  private TopicMap() {
    throw new AssertionError("not instantiable");
  }

  public static Map<String, ScopeAndCollection> parse(List<String> topicToCollection) {
    Map<String, ScopeAndCollection> result = new HashMap<>();

    for (String entry : topicToCollection) {
      String[] topicAndCollection = entry.split("=", -1);
      if (topicAndCollection.length != 2) {
        throw new IllegalArgumentException("Bad entry: '" + entry + "'. Expected exactly one equals (=) character separating topic and collection.");
      }
      String topic = topicAndCollection[0];
      ScopeAndCollection scopeAndCollection = ScopeAndCollection.parse(topicAndCollection[1]);
      result.put(topic, scopeAndCollection);
    }

    return result;
  }
}
