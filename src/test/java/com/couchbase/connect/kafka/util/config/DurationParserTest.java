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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class DurationParserTest {
    @Test
    public void parseDuration() throws Exception {
        assertEquals(0, DurationParser.parseDuration("0", MILLISECONDS));
        assertEquals(0, DurationParser.parseDuration("0s", MILLISECONDS));

        assertEquals(12345, DurationParser.parseDuration("12345ms", MILLISECONDS));

        // should round up to nearest second
        assertEquals(1, DurationParser.parseDuration("1ms", SECONDS));

        assertEquals(1, DurationParser.parseDuration("1s", SECONDS));
        assertEquals(2, DurationParser.parseDuration("2s", SECONDS));
        assertEquals(60, DurationParser.parseDuration("1m", SECONDS));
        assertEquals(60 * 60, DurationParser.parseDuration("1h", SECONDS));
        assertEquals(60 * 60 * 24, DurationParser.parseDuration("1d", SECONDS));

        assertEquals(0, DurationParser.parseDuration("0ms", MILLISECONDS));
        assertEquals(0, DurationParser.parseDuration("0ms", SECONDS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingNumber() throws Exception {
        DurationParser.parseDuration("ms", MILLISECONDS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingUnit() throws Exception {
        DurationParser.parseDuration("300", MILLISECONDS);
    }
}
