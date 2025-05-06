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

import java.util.Map;

/**
 * Holds a fallback value and zero or more context-specific values.
 * <p>
 * Specified in the connector config like this:
 * <pre>
 * some.property=fallback-value
 * some.property[context-a]=value-a
 * some.property[context-b]=value-b
 * </pre>
 *
 * @param <T> Type of the held values. Must be one of the other supported config types;
 * see {@link KafkaConfigProxyFactory}.
 * @see com.couchbase.connect.kafka.util.config.annotation.ContextDocumentation
 */
public final class Contextual<T> extends LookupTable<String, T> {
  public Contextual(
      String propertyName,
      T fallback,
      Map<String, T> contextToValue
  ) {
    super(propertyName, fallback, contextToValue);
  }
}
