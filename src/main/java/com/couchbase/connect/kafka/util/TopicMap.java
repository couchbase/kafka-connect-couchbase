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

import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

/**
 * Helper methods for parsing legacy map-like config options.
 *
 * @deprecated New map-like config options should use contextual overrides.
 * See {@link com.couchbase.connect.kafka.util.config.Contextual}.
 */
@Deprecated
public class TopicMap {
  private TopicMap() {
    throw new AssertionError("not instantiable");
  }

  public static Map<String, DocumentIdExtractor> parseTopicToDocumentId(List<String> topicToDocumentIdFormat) {
    return mapValues(parseCommon(topicToDocumentIdFormat), DocumentIdExtractor::from);
  }

  public static Map<String, Keyspace> parseTopicToCollection(
      List<String> topicToCollection,
      @Nullable String defaultBucket
  ) {
    return mapValues(parseCommon(topicToCollection), it -> Keyspace.parse(it, defaultBucket));
  }

  public static Map<ScopeAndCollection, String> parseCollectionToTopic(List<String> collectionToTopic) {
    return mapKeys(parseCommon(collectionToTopic), ScopeAndCollection::parse);
  }

  private static Map<String, String> parseCommon(List<String> map) {
    Map<String, String> result = new HashMap<>();
    for (String entry : map) {
      String[] components = entry.split("=", -1);
      if (components.length != 2) {
        throw new IllegalArgumentException("Bad entry: '" + entry + "'. Expected exactly one equals (=) character separating key and value.");
      }
      result.put(components[0], components[1]);
    }
    return result;
  }

  private static <K, V1, V2> Map<K, V2> mapValues(Map<K, V1> map, Function<? super V1, ? extends V2> valueTransformer) {
    return map.entrySet().stream()
        .collect(toMap(
            Map.Entry::getKey,
            entry -> valueTransformer.apply(entry.getValue())
        ));
  }

  private static <K1, K2, V> Map<K2, V> mapKeys(Map<K1, V> map, Function<? super K1, ? extends K2> keyTransformer) {
    return map.entrySet().stream()
        .collect(toMap(
            entry -> keyTransformer.apply(entry.getKey()),
            Map.Entry::getValue
        ));
  }
}
