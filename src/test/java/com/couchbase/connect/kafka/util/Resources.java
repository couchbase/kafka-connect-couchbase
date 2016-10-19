/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.connect.kafka.util;

import java.io.InputStream;

/**
 * Helper class for various resource handling mechanisms.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class Resources {

    /**
     * Reads a file from the resources folder (in the same path as the requesting test class).
     *
     * The class will be automatically loaded relative to the namespace and converted to a string.
     *
     * @param filename the filename of the resource.
     * @param clazz    the reference class.
     * @return the loaded string.
     */
    public static String read(final String filename, final Class<?> clazz) {
        String path = "/" + clazz.getPackage().getName().replace(".", "/") + "/" + filename;
        InputStream stream = clazz.getResourceAsStream(path);
        java.util.Scanner s = new java.util.Scanner(stream).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
