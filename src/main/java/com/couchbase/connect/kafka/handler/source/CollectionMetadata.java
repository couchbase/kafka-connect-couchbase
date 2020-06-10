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

package com.couchbase.connect.kafka.handler.source;

import com.couchbase.client.dcp.highlevel.DocumentChange;

import static java.util.Objects.requireNonNull;

public class CollectionMetadata {
  private final DocumentChange documentChange;

  public CollectionMetadata(DocumentChange documentChange) {
    this.documentChange = requireNonNull(documentChange);
  }

  public String scopeName() {
    return documentChange.getCollection().scope().name();
  }

  public long scopeId() {
    return documentChange.getCollection().scope().id();
  }

  public String collectionName() {
    return documentChange.getCollection().name();
  }

  public long collectionId() {
    return documentChange.getCollection().id();
  }
}
