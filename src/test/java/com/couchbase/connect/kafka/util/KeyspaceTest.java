/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KeyspaceTest {

  @Test
  void canParseWithNullDefaultBucket() {
    assertEquals(
        new Keyspace(null, "b", "c"),
        Keyspace.parse("b.c", null)
    );
  }

  @Test
  void emptyDefaultBucketIsSameAsNull() {
    assertEquals(
        new Keyspace(null, "b", "c"),
        Keyspace.parse("b.c", "")
    );
  }

  @Test
  void canParseWithNonNullDefaultBucket() {
    assertEquals(
        new Keyspace("a", "b", "c"),
        Keyspace.parse("b.c", "a")
    );
  }

  @Test
  void defaultBucketNameMayContainDots() {
    assertEquals(
        new Keyspace("d.b", "b", "c"),
        Keyspace.parse("b.c", "d.b")
    );
  }

  @Test
  void ignoresDefaultBucketWhenBucketIsSpecified() {
    assertEquals(
        new Keyspace("a", "b", "c"),
        Keyspace.parse("a.b.c", "x")
    );
  }

  @Test
  void unbalancedBackticksIsInvalid() {
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse("foo.bar`", null)));
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse("`foo.bar", null)));
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse("foo.`bar", null)));
  }

  @Test
  void emptyComponentIsInvalid() {
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse("", null)));
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse(".", null)));
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse(".a", null)));
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse("a.", null)));
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse("a.b.", null)));
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse(".a.b", null)));
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse("a..b", null)));
  }

  @Test
  void wrongNumberOfComponentsIsInvalid() {
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse("a", null)));
    assertThrows(IllegalArgumentException.class, (() -> Keyspace.parse("a.b.c.d", null)));
  }

  @Test
  void emptyBucketIsCanonicalizedToNull() {
    assertNull(Keyspace.parse("a.b", null).getBucket());
    assertNull(Keyspace.parse("a.b", "").getBucket());
    assertNull(new Keyspace("", "a", "b").getBucket());
  }

  @Test
  void accessorsReturnCorrectValues() {
    Keyspace ks = Keyspace.parse("db.s.c", null);
    assertEquals("db", ks.getBucket());
    assertEquals("s", ks.getScope());
    assertEquals("c", ks.getCollection());
  }

  @Test
  void canEscapeDotsInComponent() {
    assertEquals(
        new Keyspace("d.b", "s", "c"),
        Keyspace.parse("`d.b`.s.c", null)
    );

    assertEquals(
        new Keyspace("d.b", "s", "c"),
        Keyspace.parse("d`.`b.s.c", null)
    );

    assertEquals(
        new Keyspace("d.b.x", "s", "c"),
        Keyspace.parse("d`.`b`.`x.s.c", null)
    );
  }

  /**
   * This is not what a real SQL++ parser would do
   * (it would unescape a doubled backtick as a single literal backtick),
   * but literal backticks aren't valid in keyspace components,
   * and we want to avoid SQL injection issues.
   */
  @Test
  void doubledBacktickUnescapedAsEmptyString() {
    assertEquals(
        new Keyspace("bucket", "s", "c"),
        Keyspace.parse("bu``cket.s.c", null)
    );
  }

  @Test
  void canRoundTripWithBucket() {
    Keyspace original = new Keyspace("my.bucket", "s", "c");
    assertEquals(
        original,
        Keyspace.parse(original.format(), null)
    );
  }

  @Test
  void canRoundTripWithoutBucket() {
    Keyspace original = new Keyspace(null, "s", "c");
    assertEquals(
        original,
        Keyspace.parse(original.format(), null)
    );
  }
}
