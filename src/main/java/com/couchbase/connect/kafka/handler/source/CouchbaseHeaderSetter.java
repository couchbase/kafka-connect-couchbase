/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.connect.kafka.handler.source;

import com.couchbase.client.core.annotation.Stability;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Headers;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.unmodifiableMap;

@Stability.Internal
public final class CouchbaseHeaderSetter {
  private static final Map<String, Function<DocumentEvent, SchemaAndValue>> template;

  static {
    Map<String, Function<DocumentEvent, SchemaAndValue>> map = new LinkedHashMap<>();
    map.put("bucket", event -> string(event.bucket()));
    map.put("scope", event -> string(event.collectionMetadata().scopeName()));
    map.put("collection", event -> string(event.collectionMetadata().collectionName()));
    map.put("key", event -> string(event.key()));
    map.put("qualifiedKey", event -> string(event.qualifiedKey()));
    map.put("cas", event -> int64(event.cas()));
    map.put("partition", event -> int32(event.partition()));
    map.put("partitionUuid", event -> int64(event.partitionUuid()));
    map.put("seqno", event -> int64(event.bySeqno()));
    map.put("rev", event -> int64(event.revisionSeqno()));
    map.put("expiry", event -> {
          MutationMetadata md = event.mutationMetadata().orElse(null);
          return md == null || md.expiry() == 0
              ? null
              : int64(md.expiry());
        }
    );
    template = unmodifiableMap(map);
  }

  private final Map<String, Function<DocumentEvent, SchemaAndValue>> headerNameToValueAccessor;

  public static Set<String> validHeaders() {
    return template.keySet();
  }

  public CouchbaseHeaderSetter(String prefix, Collection<String> headerNames) {
    Set<String> invalidHeaderNames = new LinkedHashSet<>(headerNames);
    invalidHeaderNames.removeAll(validHeaders());
    if (!invalidHeaderNames.isEmpty()) {
      throw new IllegalArgumentException("Invalid header names: " + invalidHeaderNames + " ; each header name must be one of " + validHeaders());
    }

    Map<String, Function<DocumentEvent, SchemaAndValue>> map = new LinkedHashMap<>();
    headerNames.forEach(name -> map.put(prefix + name, template.get(name)));
    headerNameToValueAccessor = unmodifiableMap(map);
  }

  public void setHeaders(Headers headers, DocumentEvent event) {
    headerNameToValueAccessor.forEach((name, accessor) -> headers.add(name, accessor.apply(event)));
  }

  private static SchemaAndValue string(String value) {
    return new SchemaAndValue(Schema.STRING_SCHEMA, value);
  }

  private static SchemaAndValue int32(int value) {
    return new SchemaAndValue(Schema.INT32_SCHEMA, value);
  }

  private static SchemaAndValue int64(long value) {
    return new SchemaAndValue(Schema.INT64_SCHEMA, value);
  }
}
