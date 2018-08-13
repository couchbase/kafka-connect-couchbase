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

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import org.junit.Test;

import java.io.IOException;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;
import static org.junit.Assert.*;

public class DocumentPathExtractorTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final ObjectMapper lenientObjectMapper = new ObjectMapper();

    static {
        lenientObjectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }

    private static void check(
            String pointer, String document,
            String expectedDocumentId, String expectedResultDocument)
            throws IOException, DocumentPathExtractor.DocumentPathNotFoundException{
        document = toValidJson(document);
        expectedResultDocument = toValidJson(expectedResultDocument);

        DocumentPathExtractor.DocumentExtraction result = new DocumentPathExtractor(pointer, true).extractDocumentPath(document.getBytes(UTF_8));
        assertEquals(expectedDocumentId, result.getPathValue());
        assertJsonEquals(expectedResultDocument, result.getData().toString(UTF_8));

        // and again without removing the document id
        result = new DocumentPathExtractor(pointer, false).extractDocumentPath(document.getBytes(UTF_8));
        assertEquals(expectedDocumentId, result.getPathValue());
        assertEquals(document, result.getData().toString(UTF_8));

        // and one last time with lots of extra whitespace
        for (char c : ",:[]{}".toCharArray()) {
            document = document.replace(Character.toString(c), "  " + c + "  ");
        }
        result = new DocumentPathExtractor(pointer, true).extractDocumentPath(document.getBytes(UTF_8));
        assertEquals(expectedDocumentId, result.getPathValue());
        assertJsonEquals(expectedResultDocument, result.getData().toString(UTF_8));
    }

    private static void checkNotFound(String pointer, String document) throws IOException {
        document = toValidJson(document);
        final byte[] documentBytes = document.getBytes(UTF_8);

        try {
            new DocumentPathExtractor(pointer, true).extractDocumentPath(documentBytes);
            fail("expected 'not found'");
        } catch (DocumentPathExtractor.DocumentPathNotFoundException e) {
            // expected

            assertArrayEquals("path extractor must not modified the byte array when throwing exception",
                    documentBytes, document.getBytes(UTF_8));
        }
    }

    private static void assertJsonEquals(String expected, String actual) throws IOException {
        JsonNode parsedExpected = objectMapper.readTree(expected);
        JsonNode parsedActual = objectMapper.readTree(actual);
        assertEquals(parsedExpected, parsedActual);
    }


    private static DocumentPathExtractor.DocumentExtraction extract(DocumentPathExtractor extractor, String s) throws Exception {
        return extractor.extractDocumentPath(toValidJson(s).getBytes(CharsetUtil.UTF_8));
    }

    private static String toValidJson(String json) throws IOException {
        return lenientObjectMapper.readTree(json).toString();
    }

    @Test
    public void extractorIsReusable() throws Exception {
        DocumentPathExtractor extractor = new DocumentPathExtractor("/id", true);
        for (int i = 0; i < 2; i++) {
            DocumentPathExtractor.DocumentExtraction result = extract(extractor, "{'id':1}");
            assertEquals("1", result.getPathValue());
            assertJsonEquals("{}", result.getData().toString(UTF_8));
        }
    }

    @Test
    public void numericFields() throws Exception {
        String testDocument = "{'a':1, 'b':2, 'c':3}";
        check("/a", testDocument, "1", "{'b':2, 'c':3}");
        check("/b", testDocument, "2", "{'a':1, 'c':3}");
        check("/c", testDocument, "3", "{'a':1, 'b':2}");
    }

    @Test
    public void multipleNumericFields() throws Exception {
        String testDocument = "{'a':1, 'b':2, 'c':3}";
        check("${/b}-${/c}", testDocument, "2-3", "{'a':1}");
        check("${/a}-${/b}-${/c}", testDocument, "1-2-3", "{}");
    }

    @Test
    public void duplicatePlaceholders() throws Exception {
        String testDocument = "{'a':1, 'b':2, 'c':3}";
        check("${/b}-${/b}", testDocument, "2-2", "{'a':1, 'c':3}");
    }

    @Test
    public void stringFields() throws Exception {
        String testDocument = "{'a':'1', 'b':'2', 'c':'3'}";
        check("/a", testDocument, "1", "{'b':'2', 'c':'3'}");
        check("/b", testDocument, "2", "{'a':'1', 'c':'3'}");
        check("/c", testDocument, "3", "{'a':'1', 'b':'2'}");
    }

    @Test
    public void multipleStringFields() throws Exception {
        String testDocument = "{'a':'1', 'b':'2', 'c':'3'}";
        check("foo ${/b}-${/c} bar", testDocument, "foo 2-3 bar", "{'a':'1'}");
        check("foo ${/a}-${/b}-${/c} bar", testDocument, "foo 1-2-3 bar", "{}");
    }

    @Test
    public void escapedQuoteInFieldName() throws Exception {
        check("/a\"b", "{\"a\\\"b\":1}", "1", "{}");
        check("/a\"\"b", "{\"a\\\"\\\"b\":1}", "1", "{}");
        check("/a\"\\\"b", "{\"a\\\"\\\\\\\"b\":1}", "1", "{}");
    }

    @Test
    public void escapedBackslashAsLastCharOfFieldName() throws Exception {
        check("/a\\", "{\"a\\\\\":1}", "1", "{}");
    }

    @Test
    public void indexOutOfBoundsMeansNotFound() throws Exception {
        checkNotFound("/items/3", "{'items':[1,2,3]}");
    }

    @Test
    public void missingPropertyMeansNotFound() throws Exception {
        checkNotFound("/c", "{'a':{'b':3}}");
        checkNotFound("/a/c", "{'a':{'b':3}}");

        checkNotFound("${/a/b}-${/a/c}", "{'a':{'b':3}}");
    }

    @Test
    public void nonScalarMeansNotFound() throws Exception {
        checkNotFound("/a", "{'a':{'b':3}}");
    }

    @Test
    public void nullMeansNotFound() throws Exception {
        checkNotFound("/a", "{'a':null}");
    }

    @Test
    public void numericElements() throws Exception {
        String testDocument = "[1,2.5,3]";
        check("/0", testDocument, "1", "[2.5,3]");
        check("/1", testDocument, "2.5", "[1,3]");
        check("/2", testDocument, "3", "[1,2.5]");
    }

    @Test
    public void stringElements() throws Exception {
        String testDocument = "['1','2','3']";
        check("/0", testDocument, "1", "['2','3']");
        check("/1", testDocument, "2", "['1','3']");
        check("/2", testDocument, "3", "['1','2']");
    }

    @Test
    public void nestedFields() throws Exception {
        String testDocument = "{'a':1, 'b':[1,2,{'x':{'y':9}}], 'c':3}";
        check("/b/0", testDocument, "1", "{'a':1, 'b':[2,{'x':{'y':9}}], 'c':3}");
        check("/b/1", testDocument, "2", "{'a':1, 'b':[1,{'x':{'y':9}}], 'c':3}");
        check("/b/2/x/y", testDocument, "9", "{'a':1, 'b':[1,2,{'x':{}}], 'c':3}");
    }

    @Test
    public void removeLastElement() throws Exception {
        String testDocument = "[1]";
        check("/0", testDocument, "1", "[]");
    }

    @Test
    public void removeLastField() throws Exception {
        String testDocument = "{'a':1}";
        check("/a", testDocument, "1", "{}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void pointerMustNotBeEmpty() {
        new DocumentPathExtractor("", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void pointerMustBeValid() {
        new DocumentPathExtractor("a/b", true);
    }
}