/*
 * Copyright 2017 Couchbase, Inc.
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

import java.io.IOException;

/**
 * Locates a document ID using a JSON pointer, optionally removing the ID from the document.
 * <p>
 * Immutable.
 */
public class DocumentIdExtractor {

    private final DocumentPathExtractor pathExtractor;

    public DocumentIdExtractor(String documentIdFormat, boolean removeDocumentId) {
        pathExtractor = new DocumentPathExtractor(documentIdFormat,removeDocumentId);
    }

    /**
     * @param json The document content encoded as UTF-8. If this method returns normally,
     * it may modify the contents of the array to remove the fields used by the document ID.
     */
    public JsonBinaryDocument extractDocumentId(final byte[] json, int expiry) throws IOException, DocumentPathExtractor.DocumentPathNotFoundException {
        DocumentPathExtractor.DocumentExtraction extraction = pathExtractor.extractDocumentPath(json);
        return JsonBinaryDocument.create(extraction.getPathValue(), expiry, extraction.getData());
    }
}