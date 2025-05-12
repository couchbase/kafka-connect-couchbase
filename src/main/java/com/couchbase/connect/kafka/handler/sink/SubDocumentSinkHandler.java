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

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ArrayAppend;
import com.couchbase.client.java.kv.ArrayPrepend;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.Upsert;
import com.couchbase.connect.kafka.config.sink.SubDocumentSinkHandlerConfig;
import com.couchbase.connect.kafka.util.DocumentPathExtractor;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Map;

import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.StoreSemantics.REPLACE;
import static com.couchbase.client.java.kv.StoreSemantics.UPSERT;
import static com.couchbase.connect.kafka.handler.sink.N1qlSinkHandler.withAlias;
import static java.util.Collections.singletonList;

public class SubDocumentSinkHandler implements SinkHandler {
  private static final Logger log = LoggerFactory.getLogger(SubDocumentSinkHandler.class);

  private static class SubdocOperation {
    private final String id;
    private final String path;
    private final JsonObject data;

    public SubdocOperation(String id, String path, byte[] data) {
      this.id = id;
      this.path = path;
      this.data = JsonObject.fromJson(data);
    }

    public String getId() {
      return id;
    }

    public String getPath() {
      return path;
    }

    public JsonObject getData() {
      return data;
    }
  }

  private SubDocumentSinkHandlerConfig.Operation mode;
  private String path;
  private boolean createPaths;
  private boolean pathIsDynamic;
  private boolean createDocuments;

  @Override
  public void init(SinkHandlerContext context) {
    // Accept the legacy property "couchbase.create.document" as an alias for "couchbase.subdocument.create.document".
    Map<String, String> properties = withAlias(context.configProperties(),
        ConfigHelper.keyName(SubDocumentSinkHandlerConfig.class, SubDocumentSinkHandlerConfig::subdocumentCreateDocument),
        "couchbase.create.document");

    SubDocumentSinkHandlerConfig config = ConfigHelper.parse(SubDocumentSinkHandlerConfig.class, properties);
    this.mode = config.subdocumentOperation();
    this.path = config.subdocumentPath();
    this.createPaths = config.subdocumentCreatePath();
    this.createDocuments = config.subdocumentCreateDocument();

    if (this.path.trim().isEmpty()) {
      String keyName = ConfigHelper.keyName(SubDocumentSinkHandlerConfig.class, SubDocumentSinkHandlerConfig::subdocumentPath);
      throw new ConfigException("Missing required configuration \"" + keyName + "\"");
    }

    if (this.path.startsWith("/")) {
      // Interpret the given path as a JSON pointer.
      // Each Kafka message is then expected to have a field at this location;
      // the value of that field is the path to use when doing the subdoc operation.
      this.path = "${" + path + "}";
      this.pathIsDynamic = true;
    } else {
      // Interpret the path as a normal subdoc path.
      // The same path is then used for each subdoc operation.
      this.pathIsDynamic = false;
    }
  }

  @Override
  public SinkAction handle(SinkHandlerParams params) {
    String documentId = getDocumentId(params);
    SinkDocument doc = params.document().orElse(null);
    if (doc == null) {
      return SinkAction.remove(params, params.collection(), documentId);
    }

    SubdocOperation operation = getOperation(documentId, doc);

    MutateInSpec mutation;

    switch (mode) {
      case UPSERT:
        mutation = MutateInSpec.upsert(operation.getPath(), operation.getData());
        if (createPaths) {
          mutation = ((Upsert) mutation).createPath();
        }
        break;

      case ARRAY_APPEND:
        mutation = MutateInSpec.arrayAppend(operation.getPath(), singletonList(operation.getData()));
        if (createPaths) {
          mutation = ((ArrayAppend) mutation).createPath();
        }
        break;

      case ARRAY_PREPEND:
        mutation = MutateInSpec.arrayPrepend(operation.getPath(), singletonList(operation.getData()));
        if (createPaths) {
          mutation = ((ArrayPrepend) mutation).createPath();
        }
        break;

      default:
        throw new RuntimeException("Unsupported subdoc mode: " + mode);
    }

    MutateInOptions options = mutateInOptions()
        .storeSemantics(createDocuments ? UPSERT : REPLACE);
    params.expiry().ifPresent(options::expiry);
    params.configureDurability(options);

    Mono<?> action = params.collection()
        .mutateIn(documentId, singletonList(mutation), options);

    return new SinkAction(action, ConcurrencyHint.of(documentId));
  }

  private SubdocOperation getOperation(String documentId, SinkDocument doc) {
    if (!pathIsDynamic) {
      return new SubdocOperation(documentId, this.path, doc.content());
    }

    try {
      DocumentPathExtractor extractor = new DocumentPathExtractor(path);
      DocumentPathExtractor.DocumentExtraction extraction = extractor.extractDocumentPath(doc.content(), true);
      return new SubdocOperation(documentId, extraction.getPathValue(), extraction.getData());

    } catch (IOException | DocumentPathExtractor.DocumentPathNotFoundException e) {
      log.error(e.getMessage(), e);
      return new SubdocOperation(documentId, null, null);
    }
  }

  @Override
  public String toString() {
    return "SubDocumentSinkHandler{" +
        "mode=" + mode +
        ", path='" + path + '\'' +
        ", createPaths=" + createPaths +
        ", pathIsDynamic=" + pathIsDynamic +
        ", createDocuments=" + createDocuments +
        '}';
  }
}
