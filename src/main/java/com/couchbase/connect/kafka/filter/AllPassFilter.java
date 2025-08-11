/*
 * Copyright 2016 Couchbase, Inc.
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

package com.couchbase.connect.kafka.filter;

import com.couchbase.connect.kafka.handler.source.DocumentEvent;

/**
 * Allows publication of any event, except for transaction metadata
 * and events in Couchbase system scopes.
 * <p>
 * Transaction metadata is any document whose key starts with "_txn:".
 * <p>
 * A system scope is any scope whose name start with percent or underscore,
 * except for the default scope (whose name is "_default").
 *
 * @see AllPassIncludingSystemFilter
 */
public class AllPassFilter implements Filter {

  @Override
  public boolean pass(final DocumentEvent event) {
    return !isSystemScope(event) && !isTransactionMetadata(event);
  }

  private static boolean isSystemScope(DocumentEvent event) {
    String scopeName = event.collectionMetadata().scopeName();
    return scopeName.startsWith("%") || (scopeName.startsWith("_") && !scopeName.equals("_default"));
  }

  private static boolean isTransactionMetadata(DocumentEvent event) {
    return event.key().startsWith("_txn:");
  }
}
