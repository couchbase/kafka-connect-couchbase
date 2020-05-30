/*
 * Copyright 2020 Couchbase, Inc.
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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;

public class HtmlRenderer {
  static final String PARAGRAPH_SEPARATOR =
      System.lineSeparator() + System.lineSeparator();

  public static String htmlToPlaintext(String html) {
    StringBuilder result = new StringBuilder();
    html = html.replaceAll("\\s+", " ");
    renderAsPlaintext(Jsoup.parse(html).body(), result);
    trimRight(result);
    return result.toString();
  }

  private static void renderAsPlaintext(Node node, StringBuilder out) {
    if (node instanceof TextNode) {
      String text = ((TextNode) node).text();
      if (out.length() == 0 || endsWithWhitespace(out)) {
        text = trimLeft(text);
      }
      out.append(text);
      return;
    }

    if (node instanceof Element) {
      Element e = (Element) node;

      if (e.tagName().equals("p") || e.tagName().equals("br")) {
        trimRight(out);
        if (out.length() > 0) {
          out.append(PARAGRAPH_SEPARATOR);
        }
      }

      for (Node child : e.childNodes()) {
        renderAsPlaintext(child, out);
      }
    }
  }

  private static void trimRight(StringBuilder out) {
    while (endsWithWhitespace(out)) {
      out.setLength(out.length() - 1);
    }
  }

  private static String trimLeft(String text) {
    return text.replaceFirst("^\\s+", "");
  }

  private static boolean endsWithWhitespace(CharSequence out) {
    return out.length() > 0 && Character.isWhitespace(out.charAt(out.length() - 1));
  }
}
