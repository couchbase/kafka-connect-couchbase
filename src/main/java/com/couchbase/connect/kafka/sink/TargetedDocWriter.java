/*
 * Copyright (c) 2017 Couchbase, Inc.
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

package com.couchbase.connect.kafka.sink;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.*;
import com.couchbase.connect.kafka.util.DocumentPathExtractor;
import com.couchbase.connect.kafka.util.DurabilitySetter;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.StoreSemantics.REPLACE;
import static java.util.Collections.singletonList;

public class TargetedDocWriter {
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetedDocWriter.class);

    private static class DocumentOperation {
        private final String id;
        private final String path;
        private final JsonNode data;
        private final DocMutationMode mode;

        public DocumentOperation(String id, String path, JsonNode data, DocMutationMode mode) {
            this.id = id;
            this.path = path;
            this.data = data;
            this.mode = mode;
        }

        public String getId() {
            return id;
        }

        public String getPath() {
            return path;
        }

        public JsonNode getData() {
            return data;
        }

        public DocMutationMode getMode() {
            return mode;
        }
    }

    private final String path;
    private static final String OPERATION_PATH_DELIMITER = ",";

    public TargetedDocWriter(String path) {
        if (path.startsWith("/")) {
            // Interpret the given path as a JSON pointer.
            // Each Kafka message is then expected to have a field at this location;
            // the value of that field is the path to use when doing the subdoc operation.
            this.path = "${" + path + "}";
        } else {
            // Interpret the path as a normal subdoc path.
            // The same path is then used for each subdoc operation.
            this.path = path;
        }
    }

    public Mono<Void> write(final ReactiveCollection bucket, final JsonBinaryDocument document, DurabilitySetter durabilitySetter) {
        List<DocumentOperation> documentOperations = getOperations(document);

        List<MutateInSpec> mutations = new ArrayList<>();
        for (DocumentOperation operation : documentOperations){
            switch (operation.mode) {

                case UPSERT: {
                    mutations.add(MutateInSpec.upsert(operation.getPath(), operation.getData()));
                    break;
                }
                case REMOVE: {
                    mutations.add(MutateInSpec.remove(operation.getPath()));
                    break;
                }
                default:
                    throw new RuntimeException("Unsupported document mutation mode: " + operation.mode);
            }
        }

        MutateInOptions options = mutateInOptions()
                .storeSemantics(REPLACE);
        durabilitySetter.accept(options);

        return bucket.mutateIn(document.id(), mutations, options)
                .onErrorResume(DocumentNotFoundException.class, throwable -> Mono.empty())
                .then();
    }

    private List<DocumentOperation> getOperations(JsonBinaryDocument doc) {
        try {
            DocumentPathExtractor extractor = new DocumentPathExtractor(path, true);
            DocumentPathExtractor.DocumentExtraction extraction = extractor.extractDocumentPath(doc.content());
            String[] operationPaths = extraction.getPathValue().split(OPERATION_PATH_DELIMITER, -1);
            List<DocumentOperation> operations = new ArrayList<>();
            for (String opPath : operationPaths) {
                JsonNode node = findNode(doc.content(), opPath);
                DocMutationMode mode = getMode(node);
                operations.add(new DocumentOperation(doc.id(), adjustPathForMutation(opPath), node, mode));
            }
            return operations;

        } catch (IOException | DocumentPathExtractor.DocumentPathNotFoundException e) {
            LOGGER.error(e.getMessage(), e);
            return singletonList(new DocumentOperation(doc.id(), null, null, null));
        }
    }

    private JsonNode findNode(byte[] document, String path) throws IOException {
        JsonNode root = mapper.readTree(document);
        return root.at(path);
    }

    private DocMutationMode getMode(JsonNode node){
        return Objects.isNull(node) ? DocMutationMode.REMOVE : DocMutationMode.UPSERT;
    }

    private String adjustPathForMutation(String path){
        return path.replace("/",".").substring(1);
    }
}
