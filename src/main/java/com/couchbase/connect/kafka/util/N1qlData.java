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

import com.couchbase.connect.kafka.handler.sink.ConcurrencyHint;

import static java.util.Objects.requireNonNull;

public class N1qlData {
  private final String keyspace;
  private final String data;
  private final OperationType type;
  private final ConcurrencyHint hint;

  public N1qlData(String keyspace, String data, OperationType type, ConcurrencyHint hint) {
    this.keyspace = requireNonNull(keyspace);
    this.data = requireNonNull(data);
    this.type = requireNonNull(type);
    this.hint = requireNonNull(hint);
  }

  public String getData() {
    return data;
  }

  public OperationType getType() {
    return type;
  }

  public ConcurrencyHint getHint() {
    return hint;
  }

  public String getKeyspace() {
    return keyspace;
  }

  @Override
  public String toString() {
    return "[ " + "keyspace = " + keyspace + " data = " + data + " type = " + type + " hint = " + hint + " ] ";
  }

  public enum OperationType {
    UPSERT,
    DELETE
  }


}
