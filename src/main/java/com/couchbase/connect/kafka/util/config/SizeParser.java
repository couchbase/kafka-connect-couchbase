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

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SizeParser {
    private SizeParser() {
        throw new AssertionError("not instantiable");
    }

    private static final Pattern PATTERN = Pattern.compile("(\\d+)(.+)");

    private static final Map<String, Integer> qualifierToScale;

    static {
        final Map<String, Integer> temp = new HashMap<String, Integer>();
        temp.put("b", 1);
        temp.put("k", 1024);
        temp.put("m", 1024 * 1024);
        temp.put("g", 1024 * 1024 * 1024);
        qualifierToScale = Collections.unmodifiableMap(temp);
    }

    public static long parseSizeBytes(String s) {
        s = s.trim().toLowerCase(Locale.ROOT);

        if (s.equals("0")) {
            return 0;
        }

        final Matcher m = PATTERN.matcher(s);
        if (!m.matches() || !qualifierToScale.containsKey(m.group(2))) {
            throw new IllegalArgumentException("Unable to parse size '" + s + "'." +
                    " Please specify an integer followed by a size unit (b = bytes, k = kilobytes, m = megabytes, g = gigabytes)." +
                    " For example, to specify 64 megabytes: 64m");
        }

        final long value = Long.parseLong(m.group(1));
        final Integer unit = qualifierToScale.get(m.group(2));
        return value * unit;
    }
}
