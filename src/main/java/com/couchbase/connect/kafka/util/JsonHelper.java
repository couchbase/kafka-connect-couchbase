/*
 * Copyright 2025 Couchbase, Inc.
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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

public class JsonHelper {

  private JsonHelper() {
    throw new AssertionError("not instantiable");
  }

  private static final JsonFactory jsonFactory = new JsonFactory();

  public static boolean isValidJson(byte[] bytes) {
    try {
      final JsonParser parser = jsonFactory.createParser(bytes);
      final JsonToken firstToken = parser.nextToken();

      final JsonToken incrementDepthToken;
      final JsonToken decrementDepthToken;

      if (firstToken == JsonToken.START_OBJECT) {
        incrementDepthToken = JsonToken.START_OBJECT;
        decrementDepthToken = JsonToken.END_OBJECT;

      } else if (firstToken == JsonToken.START_ARRAY) {
        incrementDepthToken = JsonToken.START_ARRAY;
        decrementDepthToken = JsonToken.END_ARRAY;

      } else {
        // valid if there's exactly one token.
        return firstToken != null && parser.nextToken() == null;
      }

      int depth = 1;
      JsonToken token;
      while ((token = parser.nextToken()) != null) {
        if (token == incrementDepthToken) {
          depth++;
        } else if (token == decrementDepthToken) {
          depth--;
          if (depth == 0 && parser.nextToken() != null) {
            // multiple JSON roots, or trailing garbage
            return false;
          }
        }
      }
    } catch (IOException e) {
      // malformed
      return false;
    }

    return true;
  }
}
