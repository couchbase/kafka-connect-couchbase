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


import org.junit.jupiter.api.Test;

import static com.couchbase.connect.kafka.util.config.HtmlRenderer.PARAGRAPH_SEPARATOR;
import static com.couchbase.connect.kafka.util.config.HtmlRenderer.htmlToPlaintext;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HtmlRendererTest {
  @Test
  public void paragraphsRenderedNicely() {
    String plain = htmlToPlaintext("hello <p> world");
    assertEquals("hello" + PARAGRAPH_SEPARATOR + "world", plain);
  }

  @Test
  public void tagWhitespace() {
    assertEquals("helloworld", htmlToPlaintext("hello<b>world</b>"));
    assertEquals("helloworldagain", htmlToPlaintext("hello<b>world</b>again"));
    assertEquals("hello world", htmlToPlaintext("hello <b>world</b>"));
    assertEquals("hello world", htmlToPlaintext("hello <b> world</b>"));
    assertEquals("hello world", htmlToPlaintext("hello <b> world </b>"));
    assertEquals("hello world again", htmlToPlaintext("hello <b> world </b> again"));
  }

  @Test
  public void paragraphsBreaksAreMerged() {
    assertEquals(
        htmlToPlaintext("hello <p> world"),
        htmlToPlaintext("hello <p><p> world"));
  }

  @Test
  public void leadingAndTrailingParagraphsIgnored() {
    assertEquals("hello", htmlToPlaintext("<p><p>hello<p><p>"));
  }
}
