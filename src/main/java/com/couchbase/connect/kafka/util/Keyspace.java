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

package com.couchbase.connect.kafka.util;

import com.couchbase.connect.kafka.handler.sink.AnalyticsSinkHandler;
import com.couchbase.connect.kafka.handler.sink.SinkHandler;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.couchbase.client.core.util.CbStrings.emptyToNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Like {@link ScopeAndCollection}, but further qualified by bucket name,
 * because the sink connector can write to multiple buckets
 * (or in the case of {@link AnalyticsSinkHandler}, multiple databases).
 * <p>
 * In other words, the source connector uses ScopeAndCollection because it
 * always reads from only one bucket, and the sink connector uses Keyspace.
 */
public class Keyspace {
  private final @Nullable String bucket;
  private final String scope;
  private final String collection;
  private final String formatted;

  public static Keyspace parse(String raw, @Nullable String defaultBucket) {
    try {
      List<String> components = unescapeComponents(raw);
      components.forEach(it -> requireNonBlank(it, "keyspace component"));

      if (components.size() == 2) {
        components.add(0, emptyToNull(defaultBucket));
      }
      if (components.size() != 3) {
        throw new IllegalArgumentException(
            "Expected 2 or 3 components, but got " + components.size() +
                " ; if the bucket name contains a dot, escape it by enclosing it in backticks," +
                " like: `my.bucket`.scope.collection"
        );
      }
      return new Keyspace(components.get(0), components.get(1), components.get(2));

    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse keyspace: " + raw, e);
    }
  }

  public Keyspace(
      @Nullable String bucket,
      String scope,
      String collection
  ) {
    this.bucket = requireNoBackticks(emptyToNull(bucket));
    this.scope = requireNoBackticks(requireNonBlank(scope, "scope name"));
    this.collection = requireNoBackticks(requireNonBlank(collection, "collection name"));

    List<String> components = new ArrayList<>();
    if (bucket != null) {
      components.add(bucket);
    }
    components.add(scope);
    components.add(collection);

    this.formatted = components.stream()
        .map(it -> "`" + it + "`")
        .collect(joining("."));
  }

  private static String requireNonBlank(String s, String name) {
    requireNonNull(s, name + " is null");
    if (s.trim().isEmpty()) {
      throw new IllegalArgumentException(name + " is blank");
    }
    return s;
  }

  /**
   * Ensures we don't end up with a SQL injection issue.
   */
  private static @Nullable String requireNoBackticks(@Nullable String s) {
    if (s != null && s.contains("`")) {
      throw new IllegalArgumentException("Keyspace component may not contain a backtick (`), but got: " + s);
    }
    return s;
  }

  /**
   * In practice this is null only if the sink handler's
   * {@link SinkHandler#usesKvCollections()} method returns false
   * (like AnalyticsSourceHandler does), AND the user does not specify
   * a value for the `couchbase.bucket` config property.
   */
  public @Nullable String getBucket() {
    return bucket;
  }

  public String getScope() {
    return scope;
  }

  public String getCollection() {
    return collection;
  }

  /**
   * Returns {@code `bucket`.`scope`.`collection`} if bucket is present,
   * otherwise {@code `scope`.`collection`}
   */
  public String format() {
    return formatted;
  }

  @Override
  public String toString() {
    return "Keyspace{" +
        "bucket='" + bucket + '\'' +
        ", scope='" + scope + '\'' +
        ", collection='" + collection + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Keyspace that = (Keyspace) o;
    return Objects.equals(bucket, that.bucket) && Objects.equals(scope, that.scope) && Objects.equals(collection, that.collection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, scope, collection);
  }

  private static List<String> unescapeComponents(String raw) {
    List<String> result = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inEscapedRegion = false;
    for (char c : raw.toCharArray()) {
      if (c == '`') {
        // Unlike a full SQL++ parser, which would decode a sequence of
        // two adjacent backticks as a single literal backtick,
        // we're not handling or allowing backticks in the unescaped value.
        // That's fine, because neither KV nor Analytics allows literal
        // backticks in bucket/database, scope, or collection names.
        inEscapedRegion = !inEscapedRegion;
        continue;
      }
      if (c == '.' && !inEscapedRegion) {
        result.add(current.toString());
        current.setLength(0);
        continue;
      }
      current.append(c);
    }

    if (inEscapedRegion) {
      throw new IllegalArgumentException("Unbalanced backticks for value: " + raw);
    }

    result.add(current.toString());
    return result;
  }
}
