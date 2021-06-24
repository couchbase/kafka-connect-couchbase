/*
 * Copyright (c) 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka.example;

import com.couchbase.connect.kafka.handler.sink.SinkAction;
import com.couchbase.connect.kafka.handler.sink.SinkDocument;
import com.couchbase.connect.kafka.handler.sink.SinkHandler;
import com.couchbase.connect.kafka.handler.sink.SinkHandlerContext;
import com.couchbase.connect.kafka.handler.sink.SinkHandlerParams;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;

/**
 * An example sink handler that reads a field name from the
 * {@code "example.field.to.remove"} connector config property and removes
 * that field from every message before writing the document to Couchbase.
 */
public class CustomSinkHandler implements SinkHandler {
  private static final ObjectMapper mapper = new ObjectMapper();

  private String fieldNameToRemove;

  @Override
  public void init(SinkHandlerContext context) {
    String configPropertyName = "example.field.to.remove";
    fieldNameToRemove = context.configProperties().get(configPropertyName);
    if (fieldNameToRemove == null) {
      throw new ConfigException("Missing required connector config property: " + configPropertyName);
    }
  }

  @Override
  public SinkAction handle(SinkHandlerParams params) {
    String documentId = getDocumentId(params);
    SinkDocument doc = params.document().orElse(null);
    return doc == null
        ? SinkAction.ignore() // ignore Kafka records with null values
        : SinkAction.upsertJson(params, params.collection(), documentId, transform(doc.content()));
  }

  private byte[] transform(byte[] content) {
    try {
      JsonNode node = mapper.readTree(content);

      if (node.has(fieldNameToRemove)) {
        ((ObjectNode) node).remove(fieldNameToRemove);
        return mapper.writeValueAsBytes(node);
      }

      return content;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
