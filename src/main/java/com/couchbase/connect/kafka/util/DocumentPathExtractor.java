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
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Locates a document ID using a JSON pointer, optionally removing the ID from the document.
 * <p>
 * Immutable.
 */
public class DocumentPathExtractor {

    public static class DocumentPathNotFoundException extends Exception {
        DocumentPathNotFoundException(String message) {
            super(message);
        }
    }

    public class DocumentExtraction {
        private String pathValue;
        private ByteBuf data;

        DocumentExtraction(String pathValue, byte[] data) {
            this.pathValue = pathValue;
            this.data = Unpooled.wrappedBuffer(data);
        }

        public String getPathValue(){
            return pathValue;
        }

        public ByteBuf getData() {
            return data;
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

        void fill(byte[] bytes, byte fillByte) {
            Arrays.fill(bytes, startOffset, pastEndOffset, fillByte);
        }
    }

    private static final JsonFactory factory = new JsonFactory();
    private final String documentPathFormat;
    private final Map<String, JsonPointer> placeholderToJsonPointer = new HashMap<String, JsonPointer>();

    private final boolean removeDocumentPath;

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{(.+?)}");

    public DocumentPathExtractor(String documentPathFormat, boolean removeDocumentPath) {
        if (documentPathFormat.isEmpty()) {
            throw new IllegalArgumentException("Document ID format must not be empty");
        }

        Matcher m = PLACEHOLDER_PATTERN.matcher(documentPathFormat);
        if (!m.find()) {
            // For backwards compatibility, treat the whole thing as a single JSON pointer
            documentPathFormat = "${" + documentPathFormat + "}";
            m = PLACEHOLDER_PATTERN.matcher(documentPathFormat);
            if (!m.find()) {
                // Shouldn't happen, since we just added the placeholder delimiters
                throw new AssertionError("invalid document ID format string");
            }
        }

        do {
            final String placeholder = m.group();
            final String jsonPointer = m.group(1);
            placeholderToJsonPointer.put(placeholder, JsonPointer.compile(jsonPointer));
        } while (m.find());

        this.documentPathFormat = documentPathFormat;
        this.removeDocumentPath = removeDocumentPath;
    }

    /**
     * @param json The document content encoded as UTF-8. If this method returns normally,
     * it may modify the contents of the array to remove the fields used by the document ID.
     */
    public DocumentExtraction extractDocumentPath(final byte[] json) throws IOException, DocumentPathNotFoundException {
        final List<ByteRange> rangesToRemove = new ArrayList<ByteRange>(placeholderToJsonPointer.size());

        String documentId = documentPathFormat;

        for (Map.Entry<String, JsonPointer> idComponent : placeholderToJsonPointer.entrySet()) {
            final String placeholder = idComponent.getKey();
            final JsonPointer documentIdPointer = idComponent.getValue();

            final JsonParser parser = new FilteringParserDelegate(
                    factory.createParser(json), new JsonPointerBasedFilter(documentIdPointer), false, false);

            if (parser.nextToken() == null) {
                throw new DocumentPathNotFoundException("Document has no value matching JSON pointer '" + documentIdPointer + "'");
            }

            final String component = parser.getValueAsString();
            if (component == null) {
                throw new DocumentPathNotFoundException("The value matching JSON pointer '" + documentIdPointer + "' is null or non-scalar.");
            }

            documentId = documentId.replace(placeholder, component);

            if (removeDocumentPath) {
                rangesToRemove.add(ByteRange.forCurrentToken(json, parser));
            }
        }

        // At this point we're sure DocumentIdNotFoundException wasn't thrown, and we can expect
        // this method to return normally. It is finally safe to modify the document content.
        for (ByteRange range : rangesToRemove) {
            swallowFieldName(range);
            swallowOneComma(range);
            range.fill(json, (byte) ' ');
        }

        return new DocumentExtraction(documentId ,json);
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