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

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonFactory;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonPointer;
import com.couchbase.client.deps.com.fasterxml.jackson.core.filter.FilteringParserDelegate;
import com.couchbase.client.deps.com.fasterxml.jackson.core.filter.JsonPointerBasedFilter;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

import java.io.IOException;

/**
 * Locates a document ID using a JSON pointer, optionally removing the ID from the document.
 * <p>
 * Immutable.
 */
public class DocumentIdExtractor {

    public static class DocumentIdNotFoundException extends Exception {
        public DocumentIdNotFoundException(String message) {
            super(message);
        }
    }

    private static class ByteRange {
        private final byte[] bytes;
        private int startOffset;
        private int pastEndOffset;

        private ByteRange(byte[] bytes, int startOffset, int pastEndOffset) {
            this.bytes = bytes;
            this.startOffset = startOffset;
            this.pastEndOffset = pastEndOffset;
        }

        private static ByteRange forCurrentToken(byte[] bytes, JsonParser parser) {
            return new ByteRange(bytes,
                    (int) parser.getTokenLocation().getByteOffset(),
                    (int) parser.getCurrentLocation().getByteOffset());
        }

        @Override
        public String toString() {
            return "[" + startOffset + "," + pastEndOffset + ") = |" + new String(bytes, startOffset, pastEndOffset - startOffset) + "|";
        }
    }

    private static final JsonFactory factory = new JsonFactory();
    private final JsonPointer documentIdPointer;
    private final boolean removeDocumentId;

    public DocumentIdExtractor(JsonPointer documentIdPointer, boolean removeDocumentId) {
        if (documentIdPointer.matches()) {
            throw new IllegalArgumentException("Empty JSON pointer not supported");
        }

        this.documentIdPointer = documentIdPointer;
        this.removeDocumentId = removeDocumentId;
    }

    public DocumentIdExtractor(String documentIdPointer, boolean removeDocumentId) {
        this(JsonPointer.compile(documentIdPointer), removeDocumentId);
    }

    public JsonBinaryDocument extractDocumentId(final byte[] json) throws IOException, DocumentIdNotFoundException {
        JsonParser parser = factory.createParser(json);
        parser = new FilteringParserDelegate(parser, new JsonPointerBasedFilter(documentIdPointer), false, false);

        if (parser.nextToken() == null) {
            throw new DocumentIdNotFoundException("Document has no value matching JSON pointer '" + documentIdPointer + "'");
        }

        String documentId = parser.getValueAsString();
        if (documentId == null) {
            throw new DocumentIdNotFoundException("The value matching JSON pointer '" + documentIdPointer + "' is null or non-scalar.");
        }

        if (!removeDocumentId) {
            return JsonBinaryDocument.create(documentId, json);
        }

        ByteRange removalRange = ByteRange.forCurrentToken(json, parser);
        swallowFieldName(removalRange);
        swallowOneComma(removalRange);

        return JsonBinaryDocument.create(documentId, Unpooled.wrappedBuffer(
                Unpooled.wrappedBuffer(json, 0, removalRange.startOffset),
                Unpooled.wrappedBuffer(json, removalRange.pastEndOffset, json.length - removalRange.pastEndOffset)));
    }

    private static void swallowOneComma(ByteRange range) {
        swallowWhitespace(range);

        if (range.bytes[range.pastEndOffset] == ',') {
            range.pastEndOffset++;

        } else if (range.bytes[range.startOffset - 1] == ',') {
            range.startOffset--;
        }
    }

    private static void swallowWhitespace(ByteRange range) {
        swallowWhitespaceLeft(range);
        swallowWhitespaceRight(range);
    }

    private static void swallowWhitespaceLeft(ByteRange range) {
        while (isJsonWhitespace(range.bytes[range.startOffset - 1])) {
            range.startOffset--;
        }
    }

    private static void swallowWhitespaceRight(ByteRange range) {
        while (isJsonWhitespace(range.bytes[range.pastEndOffset])) {
            range.pastEndOffset++;
        }
    }

    private static void swallowFieldName(ByteRange range) {
        swallowWhitespaceLeft(range);

        // If the target was a field, then prevChar will be the colon (:) that separates the field name from value.
        // If the target was an array element, then prevChar will be the array start token ([) or the comma (,)
        // separating the target from the previous array element.

        byte prevChar = range.bytes[range.startOffset - 1];
        if (prevChar == ':') {
            range.startOffset--; // swallow colon
            swallowWhitespaceLeft(range);
            range.startOffset--; // swallow field name closing quote

            // swallow left to include field name opening quote (guaranteed to not be preceded by backslash)
            do {
                range.startOffset--;
            } while (!(range.bytes[range.startOffset] == '"' && range.bytes[range.startOffset - 1] != '\\'));
        }
    }

    private static boolean isJsonWhitespace(byte b) {
        switch (b) {
            case 0x20: // Space
            case 0x09: // Horizontal tab
            case 0x0A: // LF
            case 0x0D: // CR
                return true;
            default:
                return false;
        }
    }
}
