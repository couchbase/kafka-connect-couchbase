/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.connect.kafka.util.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SizeParserTest {
    @Test
    public void parseSize() throws Exception {
        assertEquals(0, SizeParser.parseSizeBytes("0"));
        assertEquals(0, SizeParser.parseSizeBytes("0k"));
        assertEquals(3, SizeParser.parseSizeBytes("3b"));
        assertEquals(3 * 1024, SizeParser.parseSizeBytes("3k"));
        assertEquals(3 * 1024 * 1024, SizeParser.parseSizeBytes("3m"));
        assertEquals(3L * 1024 * 1024 * 1024, SizeParser.parseSizeBytes("3g"));

        assertEquals(0, SizeParser.parseSizeBytes("0b"));
        assertEquals(0, SizeParser.parseSizeBytes("0k"));
        assertEquals(0, SizeParser.parseSizeBytes("0m"));
        assertEquals(0, SizeParser.parseSizeBytes("0g"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingNumber() throws Exception {
        SizeParser.parseSizeBytes("k");
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingUnit() throws Exception {
        SizeParser.parseSizeBytes("300");
    }
}
