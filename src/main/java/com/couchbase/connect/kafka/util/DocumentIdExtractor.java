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

import com.couchbase.connect.kafka.handler.sink.SinkDocument;

import java.io.IOException;

/**
 * Locates a document ID using a JSON pointer, optionally removing the ID from the document.
 * <p>
 * Immutable.
 */
public interface DocumentIdExtractor {

  /**
   * @param json The document content encoded as UTF-8. If this method returns normally,
   * it may modify the contents of the array to remove the fields used by the document ID.
   */
  SinkDocument extractDocumentId(final byte[] json, boolean removeDocumentId) throws IOException, DocumentPathExtractor.DocumentPathNotFoundException;

  static DocumentIdExtractor from(String documentIdFormat) {
    if (documentIdFormat.isEmpty()) {
      return (json, removeDocumentPath) -> new SinkDocument(null, json);
    }

    DocumentPathExtractor pathExtractor = new DocumentPathExtractor(documentIdFormat);

    return (json, removeDocumentId) -> {
      DocumentPathExtractor.DocumentExtraction extraction = pathExtractor.extractDocumentPath(json, removeDocumentId);
      return new SinkDocument(extraction.getPathValue(), extraction.getData());
    };
  }
}
