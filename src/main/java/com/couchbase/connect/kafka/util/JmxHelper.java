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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbStrings.removeStart;

/**
 * Utilities for integrating Micrometer with JMX.
 * <p>
 * As much as possible, follows
 * <a href="http://www.oracle.com/technetwork/java/javase/tech/best-practices-jsp-136021.html">
 * Java Management Extensions (JMX) Best Practices</a>.
 * <p>
 * Specifically:
 * <ul>
 *   <li>All objects of the same "type" have the same set of attributes.
 *   <li>If it's possible for a key property to have a value that requires
 *   escaping, then *all* values of that property should be escaped.
 * </ul>
 * <p>
 * These utilities expose the tags of multi-dimensional metrics as JMX key properties.
 * The intent is to let users query the JMX objects in much the same way you might filter
 * the metrics across different dimensions in a more sophisticated monitoring system.
 */
public class JmxHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(JmxHelper.class);

  private JmxHelper() {
    throw new AssertionError("not instantiable");
  }

  public static JmxMeterRegistry newJmxMeterRegistry(String domain, LinkedHashMap<String, String> commonKeyProperties) {
    MetricRegistry registry = new MetricRegistry();
    String namePrefix = domain + ":" + format(commonKeyProperties);
    JmxReporter reporter = JmxReporter.forRegistry(registry)
        .createsObjectNamesWith(preformattedObjectNameFactory(namePrefix))
        .build();

    return new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM, jmxNameMapper("dcp"), registry, reporter);
  }

  /**
   * Returns a name mapper that converts a meter ID (meter type, name and tags)
   * into a preformatted list of JMX key properties, suitable for direct inclusion in
   * a JMX ObjectName.
   *
   * @param groups list of metric name prefixes that should be interpreted as "groups"
   */
  private static HierarchicalNameMapper jmxNameMapper(String... groups) {
    return (id, convention) -> {
      String canonicalName = id.getType().name().toLowerCase(Locale.ROOT).replace("_", ".");
      String type = convention.name(canonicalName, id.getType());

      LinkedHashMap<String, String> keyProperties = new LinkedHashMap<>();
      keyProperties.put("type", type);

      String name = id.getName();
      for (String group : groups) {
        String prefix = group + ".";
        if (id.getName().startsWith(prefix)) {
          name = removeStart(name, prefix);
          keyProperties.put("group", group);
          break;
        }
      }

      keyProperties.put("name", convention.name(name, id.getType()));

      // JMX object names aren't *really* hierarchical... but some browsers display them that way.
      // Give those browsers a hint about the best hierarchy.
      LinkedHashMap<String, String> orderedTags = toMapWithOrderedKeys(id.getTagsAsIterable(),
          "remote", "opcode", "result", "exception");

      // Remove useless ActionCounter tags; JMX doesn't care if
      // two metrics with the same name have different tag names.
      if ("success".equals(orderedTags.get("result"))) {
        orderedTags.remove("exception"); // always "none", and clutters JMX hierarchy
      }

      for (Map.Entry<String, String> tag : orderedTags.entrySet()) {
        keyProperties.put(
            convention.name(tag.getKey(), id.getType()),
            ObjectName.quote(tag.getValue()));
      }

      return format(keyProperties);
    };
  }

  private static String format(Map<String, String> keyProperties) {
    // Assumes values that require quoting have already been quoted.
    return keyProperties.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining(","));
  }

  private static LinkedHashMap<String, String> toMapWithOrderedKeys(Iterable<Tag> tags, String... orderedKeys) {
    LinkedHashMap<String, String> tagMap = new LinkedHashMap<>();
    for (Tag tag : tags) {
      tagMap.put(tag.getKey(), tag.getValue());
    }
    return withKeysInOrder(tagMap, orderedKeys);
  }

  @SafeVarargs
  private static <K, V> LinkedHashMap<K, V> withKeysInOrder(Map<K, V> map, K... orderedKeys) {
    LinkedHashMap<K, V> temp = new LinkedHashMap<>(map);
    LinkedHashMap<K, V> result = new LinkedHashMap<>();
    for (K key : orderedKeys) {
      V value = temp.remove(key);
      if (value != null) {
        result.put(key, value);
      }
    }
    result.putAll(temp);
    return result;
  }

  /**
   * Returns a name factory that assumes metric names are well-formed JMX key property lists.
   * Intended for use with {@link #jmxNameMapper}
   *
   * @param objectNamePrefix the JMX domain, followed by a colon, optionally followed by
   * a JMX key property list to be prepended to all object names.
   */
  private static ObjectNameFactory preformattedObjectNameFactory(String objectNamePrefix) {
    return (type, domain, name) -> {
      try {
        return ObjectName.getInstance(objectNamePrefix + "," + name);

      } catch (MalformedObjectNameException e) {
        LOGGER.error("failed to create ObjectName for metric '{}'", name);
        throw new RuntimeException(e);
      }
    };
  }
}
