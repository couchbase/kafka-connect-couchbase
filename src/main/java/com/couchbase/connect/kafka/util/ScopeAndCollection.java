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

import com.couchbase.connect.kafka.handler.source.DocumentEvent;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Like a {@link Keyspace} where the database component is always null.
 * It's a distinct type (at least for now) to reinforce the idea that
 * the source connector can stream from only one bucket.
 * <p>
 * In other words, the source connector uses ScopeAndCollection because
 * it reads from only one bucket, and the sink connector uses Keyspace
 * because it can write to multiple buckets.
 */
public class ScopeAndCollection {
  private final String scope;
  private final String collection;

  public static ScopeAndCollection parse(String scopeAndCollection) {
    try {
      Keyspace ks = Keyspace.parse(scopeAndCollection, null);
      if (ks.getBucket() != null) {
        throw new IllegalArgumentException("Expected 2 components, but got 3; a bucket name is not valid in this context.");
      }
      return new ScopeAndCollection(ks.getScope(), ks.getCollection());
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Expected a qualified collection name (scope.collection) with no bucket component, but got: " + scopeAndCollection,
          e
      );
    }
  }

  public static ScopeAndCollection from(DocumentEvent docEvent) {
    return new ScopeAndCollection(docEvent.collectionMetadata().scopeName(), docEvent.collectionMetadata().collectionName());
  }

  public ScopeAndCollection(String scope, String collection) {
    this.scope = requireNonNull(scope);
    this.collection = requireNonNull(collection);
  }

  public String getScope() {
    return scope;
  }

  public String getCollection() {
    return collection;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ScopeAndCollection that = (ScopeAndCollection) o;
    return scope.equals(that.scope) &&
        collection.equals(that.collection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, collection);
  }
}
