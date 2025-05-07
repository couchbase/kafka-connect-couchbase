/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka.util.config;

import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.couchbase.client.core.util.CbCollections.mapCopyOf;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * A read-only map that returns a default value for absent keys.
 */
public class LookupTable<K, V> {
  private final String propertyName;
  private final V defaultValue;
  private final Map<K, V> map;

  LookupTable(String propertyName, V defaultValue, Map<K, V> map) {
    this.propertyName = requireNonNull(propertyName);
    this.defaultValue = requireNonNull(defaultValue);
    this.map = mapCopyOf(map);
  }

  public V get(K key) {
    return map.getOrDefault(key, defaultValue);
  }

  public LookupTable<K, V> withUnderlay(Map<K, V> contexts) {
    Map<K, V> newMap = new HashMap<>(contexts);
    newMap.putAll(map);
    return new LookupTable<>(propertyName, defaultValue, newMap);
  }

  public <K2> LookupTable<K2, V> mapKeys(Function<String, K2> keyMapper) {
    Map<K2, V> newMap = map.entrySet().stream()
        .collect(toMap(
            entry -> {
              try {
                return keyMapper.apply((String) entry.getKey());
              } catch (RuntimeException e) {
                throw new ConfigException("Invalid configuration " + propertyName + "[" + entry.getKey() + "] ; " + e);
              }
            },
            Map.Entry::getValue
        ));
    return new LookupTable<>(propertyName, defaultValue, newMap);
  }

  public <V2> LookupTable<K, V2> mapValues(Function<V, V2> valueMapper) {
    Map<K, V2> newMap = map.entrySet().stream()
        .collect(toMap(
            Map.Entry::getKey,
            entry -> mapOneValue(propertyName + "[" + entry.getKey() + "]", entry.getValue(), valueMapper)
        ));

    return new LookupTable<>(
        propertyName,
        mapOneValue(propertyName, defaultValue, valueMapper),
        newMap);
  }

  private static <V, R> R mapOneValue(String propertyNameWithContext, V value, Function<V, R> valueMapper) {
    try {
      return valueMapper.apply(value);
    } catch (RuntimeException e) {
      throw new ConfigException(propertyNameWithContext, value, e.toString());
    }
  }

  @Override
  public String toString() {
    return "LookupTable{" +
        "defaultValue=" + defaultValue +
        ", map=" + map +
        '}';
  }
}
