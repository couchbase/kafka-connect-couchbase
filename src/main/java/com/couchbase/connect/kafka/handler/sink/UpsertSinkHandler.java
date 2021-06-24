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

/**
 * Upserts each incoming Kafka record to Couchbase as a JSON document
 * using the Key/Value (Data) service.
 */
public class UpsertSinkHandler implements SinkHandler {
  @Override
  public SinkAction handle(SinkHandlerParams params) {
    String documentId = getDocumentId(params);
    SinkDocument doc = params.document().orElse(null);
    return doc == null
        ? SinkAction.remove(params, params.collection(), documentId)
        : SinkAction.upsertJson(params, params.collection(), documentId, doc.content());
  }

  @Override
  public String toString() {
    return "UpsertSinkHandler{}";
  }
}
