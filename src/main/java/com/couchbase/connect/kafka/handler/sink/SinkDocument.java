/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.connect.kafka.handler.sink;

import org.jspecify.annotations.Nullable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Holds the content of a document, and optionally a document ID
 * derived from fields of the document.
 */
public class SinkDocument {
  private final @Nullable String id;
  private final byte[] content;

  public SinkDocument(@Nullable String id, byte[] content) {
    this.id = id;
    this.content = requireNonNull(content);
  }

  /**
   * Returns the document ID extracted from the message body,
   * or an empty optional if ID extraction is disabled or failed
   * because the expected field(s) are missing.
   */
  public Optional<String> id() {
    return Optional.ofNullable(id);
  }

  /**
   * Returns the document content. If the connector is configured
   * to remove fields used to generate the document ID, those fields
   * will not be present in the returned content.
   */
  public byte[] content() {
    return content;
  }
}
