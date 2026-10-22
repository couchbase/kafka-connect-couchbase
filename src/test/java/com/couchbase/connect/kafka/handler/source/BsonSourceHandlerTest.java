/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.connect.kafka.handler.source;

import com.couchbase.client.dcp.highlevel.DocumentChange;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BsonSourceHandlerTest {

  private static BsonDocument convert(String json) {
    byte[] bson = BsonSourceHandler.jsonEnvelopeToBson(json.getBytes(UTF_8));
    return new BsonDocumentCodec().decode(
        new BsonBinaryReader(ByteBuffer.wrap(bson)),
        DecoderContext.builder().build());
  }

  private static String env(long rank, String value, long type) {
    return "{\"r\":" + rank + ",\"v\":" + value + ",\"z\":" + type + "}";
  }

  @Test
  public void scalarTypes() {
    BsonDocument doc = convert("{" +
        "\"_id\":" + env(8, "\"507f1f77bcf86cd799439011\"", 7) + "," +
        "\"s\":" + env(4, "\"hello\"", 2) + "," +
        "\"d\":" + env(3, "2.5", 1) + "," +
        "\"i32\":" + env(3, "42", 16) + "," +
        "\"i64\":" + env(3, "1760000000000000000", 18) + "," +
        "\"dec\":" + env(3, "\"1.10\"", 19) + "," +
        "\"b\":" + env(9, "true", 8) + "," +
        "\"date\":" + env(10, "1700000000000", 9) + "," +
        "\"n\":" + env(2, "null", 10) + "," +
        "\"undef\":" + env(1, "null", 6) + "," +
        "\"min\":" + env(0, "null", 255) + "," +
        "\"max\":" + env(13, "null", 127) +
        "}");

    assertEquals(new ObjectId("507f1f77bcf86cd799439011"), doc.getObjectId("_id").getValue());
    assertEquals("hello", doc.getString("s").getValue());
    assertEquals(2.5, doc.getDouble("d").getValue());
    assertEquals(BsonType.INT32, doc.get("i32").getBsonType());
    assertEquals(42, doc.getInt32("i32").getValue());
    assertEquals(1760000000000000000L, doc.getInt64("i64").getValue());
    assertEquals(Decimal128.parse("1.10"), doc.getDecimal128("dec").getValue());
    assertTrue(doc.getBoolean("b").getValue());
    assertEquals(1700000000000L, doc.getDateTime("date").getValue());
    assertTrue(doc.get("n").isNull());
    assertEquals(BsonType.UNDEFINED, doc.get("undef").getBsonType());
    assertEquals(BsonType.MIN_KEY, doc.get("min").getBsonType());
    assertEquals(BsonType.MAX_KEY, doc.get("max").getBsonType());
  }

  @Test
  public void nonFiniteDoubles() {
    // JSON can't express these, so they are null payloads told apart by type code.
    BsonDocument doc = convert("{" +
        "\"nan\":" + env(3, "null", 201) + "," +
        "\"neg\":" + env(3, "null", 202) + "," +
        "\"pos\":" + env(3, "null", 203) +
        "}");

    assertTrue(Double.isNaN(doc.getDouble("nan").getValue()));
    assertEquals(Double.NEGATIVE_INFINITY, doc.getDouble("neg").getValue());
    assertEquals(Double.POSITIVE_INFINITY, doc.getDouble("pos").getValue());
  }

  @Test
  public void binaryPreservesSubtype() {
    // data is 01 02 03; subtypes 04 (UUID), 80 (user-defined), 00 (generic)
    BsonDocument doc = convert("{" +
        "\"uuid\":" + env(7, "{\"l\":3,\"s\":\"04\",\"z\":\"AQID\"}", 5) + "," +
        "\"user\":" + env(7, "{\"l\":3,\"s\":\"80\",\"z\":\"AQID\"}", 5) + "," +
        "\"generic\":" + env(7, "{\"l\":3,\"s\":\"00\",\"z\":\"AQID\"}", 5) +
        "}");

    assertEquals(4, doc.getBinary("uuid").getType());
    assertEquals((byte) 0x80, doc.getBinary("user").getType());
    assertEquals(0, doc.getBinary("generic").getType());
    assertArrayEquals(new byte[]{1, 2, 3}, doc.getBinary("uuid").getData());
  }

  /**
   * Returns {subtype, body} of the sole binary field "b" in the BSON document,
   * read from the raw bytes so nothing is re-interpreted by a reader.
   */
  private static byte[][] rawBinaryField(String envelopeJson) {
    byte[] bson = BsonSourceHandler.jsonEnvelopeToBson(envelopeJson.getBytes(UTF_8));
    ByteBuffer buf = ByteBuffer.wrap(bson).order(java.nio.ByteOrder.LITTLE_ENDIAN);
    buf.position(4); // document length
    assertEquals(0x05, buf.get());
    assertEquals('b', buf.get());
    assertEquals(0, buf.get());
    int length = buf.getInt();
    byte subtype = buf.get();
    byte[] body = new byte[length];
    buf.get(body);
    return new byte[][]{{subtype}, body};
  }

  @Test
  public void everyBinarySubtypeIsPreservedByteForByte() {
    // Includes subtype 02 (deprecated "old binary"), whose body starts with its own
    // length prefix and so must be passed through untouched.
    byte[] data = {4, 0, 0, 0, (byte) 0xaa, (byte) 0xbb, (byte) 0xcc, (byte) 0xdd};
    String base64 = java.util.Base64.getEncoder().encodeToString(data);

    for (int subtype = 0; subtype <= 0xFF; subtype++) {
      String envelope = "{\"b\":" + env(7, String.format(
          "{\"l\":%d,\"s\":\"%02x\",\"z\":\"%s\"}", data.length, subtype, base64), 5) + "}";
      byte[][] raw = rawBinaryField(envelope);
      assertEquals((byte) subtype, raw[0][0], "subtype " + subtype);
      assertArrayEquals(data, raw[1], "body of subtype " + subtype);
    }
  }

  @Test
  public void largeBinaryExceedingDefaultJacksonStringLimit() {
    // ~15.8 MB fits in a MongoDB document (16 MiB) but its base64 form is over
    // 20M characters, Jackson's default maximum string length.
    byte[] data = new byte[15_800_000];
    java.util.Arrays.fill(data, (byte) 0x5a);
    String base64 = java.util.Base64.getEncoder().encodeToString(data);
    assertTrue(base64.length() > 20_000_000);

    BsonDocument doc = convert("{\"b\":" + env(7, String.format(
        "{\"l\":%d,\"s\":\"00\",\"z\":\"%s\"}", data.length, base64), 5) + "}");

    assertArrayEquals(data, doc.getBinary("b").getData());
  }

  @Test
  public void rejectsOldBinaryWithInconsistentLengthPrefix() {
    // subtype 02 data is 4-byte prefix + payload; here the prefix says 9 but 4 bytes follow
    byte[] data = {9, 0, 0, 0, 1, 2, 3, 4};
    String base64 = java.util.Base64.getEncoder().encodeToString(data);
    assertThrows(DataException.class, () -> convert("{\"b\":" + env(7,
        "{\"l\":8,\"s\":\"02\",\"z\":\"" + base64 + "\"}", 5) + "}"));
    assertThrows(DataException.class, () -> convert("{\"b\":" + env(7,
        "{\"l\":0,\"s\":\"02\",\"z\":\"\"}", 5) + "}"));
  }

  @Test
  public void numericExtremesAndEmptyValues() {
    BsonDocument doc = convert("{" +
        "\"f\":" + env(9, "false", 8) + "," +
        "\"i32min\":" + env(3, "-2147483648", 16) + "," +
        "\"i32max\":" + env(3, "2147483647", 16) + "," +
        "\"i64min\":" + env(3, "-9223372036854775808", 18) + "," +
        "\"i64max\":" + env(3, "9223372036854775807", 18) + "," +
        "\"negDate\":" + env(10, "-1", 9) + "," +
        "\"negZero\":" + env(3, "-0.0", 1) + "," +
        "\"emptyStr\":" + env(4, "\"\"", 2) + "," +
        "\"emptyDoc\":" + env(5, "{}", 3) + "," +
        "\"emptyArr\":" + env(6, "[]", 4) + "," +
        "\"emptyBin\":" + env(7, "{\"l\":0,\"s\":\"00\",\"z\":\"\"}", 5) + "," +
        "\"unicode\":" + env(4, "\"h\\u00e9llo \\ud83d\\ude00\"", 2) +
        "}");

    assertFalse(doc.getBoolean("f").getValue());
    assertEquals(Integer.MIN_VALUE, doc.getInt32("i32min").getValue());
    assertEquals(Integer.MAX_VALUE, doc.getInt32("i32max").getValue());
    assertEquals(Long.MIN_VALUE, doc.getInt64("i64min").getValue());
    assertEquals(Long.MAX_VALUE, doc.getInt64("i64max").getValue());
    assertEquals(-1, doc.getDateTime("negDate").getValue());
    assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(doc.getDouble("negZero").getValue()));
    assertEquals("", doc.getString("emptyStr").getValue());
    assertTrue(doc.getDocument("emptyDoc").isEmpty());
    assertTrue(doc.getArray("emptyArr").isEmpty());
    assertEquals(0, doc.getBinary("emptyBin").getData().length);
    assertEquals("h\u00e9llo \ud83d\ude00", doc.getString("unicode").getValue());
  }

  @Test
  public void storedErrorMarkerIsRejected() {
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(99, "\"boom\"", -1) + "}"));
  }

  @Test
  public void uuidHexString() {
    BsonDocument doc = convert("{\"u\":" + env(7, "\"000102030405060708090a0b0c0d0e0f\"", 200) + "}");
    assertEquals(4, doc.getBinary("u").getType());
    assertEquals(16, doc.getBinary("u").getData().length);
    assertEquals(15, doc.getBinary("u").getData()[15]);
    assertEquals(java.util.UUID.fromString("00010203-0405-0607-0809-0a0b0c0d0e0f"), doc.getBinary("u").asUuid());
  }

  @Test
  public void rejectsUuidHexStringOfWrongLength() {
    for (String hex : new String[]{"", "00", "000102030405060708090a0b0c0d0e", "000102030405060708090a0b0c0d0e0f10"}) {
      assertThrows(DataException.class, () -> convert("{\"u\":" + env(7, "\"" + hex + "\"", 200) + "}"), hex);
    }
    // right length, not hex
    assertThrows(DataException.class, () -> convert("{\"u\":" + env(7, "\"000102030405060708090a0b0c0d0eZZ\"", 200) + "}"));
  }

  @Test
  public void regex() {
    BsonDocument doc = convert("{\"re\":" + env(12, "{\"p\":\"^a\",\"z\":\"i\"}", 11) + "}");
    assertEquals("^a", doc.getRegularExpression("re").getPattern());
    assertEquals("i", doc.getRegularExpression("re").getOptions());
  }

  @Test
  public void timestamp() {
    // t is seconds and z is the increment; both are unsigned 32-bit
    BsonDocument doc = convert("{\"ts\":" + env(11, "{\"t\":4294967295,\"z\":7}", 17) + "}");
    assertEquals(-1, doc.getTimestamp("ts").getTime());
    assertEquals(7, doc.getTimestamp("ts").getInc());
  }

  @Test
  public void decimal128KeepsFullPrecision() {
    BsonDocument doc = convert("{\"a\":" + env(3, "\"0.1234567890123456789\"", 19) + "}");
    assertEquals(Decimal128.parse("0.1234567890123456789"), doc.getDecimal128("a").getValue());
  }

  @Test
  public void rejectsNumericDecimal128() {
    // A JSON number would be parsed as a double, silently losing precision.
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(3, "1.10", 19) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(3, "0.1234567890123456789", 19) + "}"));
  }

  @Test
  public void rejectsOutOfRangeTimestamp() {
    // one past the unsigned 32-bit maximum must not wrap around to zero
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(11, "{\"t\":4294967296,\"z\":0}", 17) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(11, "{\"t\":0,\"z\":4294967296}", 17) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(11, "{\"t\":0,\"z\":-1}", 17) + "}"));
  }

  @Test
  public void nestedDocumentsAndArrays() {
    BsonDocument doc = convert("{\"o\":" + env(5,
        "{\"a\":" + env(4, "\"x\"", 2) + ",\"list\":" + env(6,
            "[" + env(3, "1", 16) + "," + env(4, "\"y\"", 2) + "]", 4) + "}", 3) + "}");

    BsonDocument o = doc.getDocument("o");
    assertEquals("x", o.getString("a").getValue());
    assertEquals(1, o.getArray("list").get(0).asInt32().getValue());
    assertEquals("y", o.getArray("list").get(1).asString().getValue());
  }

  @Test
  public void fieldsNamedLikeEnvelopeKeys() {
    BsonDocument doc = convert("{\"r\":" + env(4, "\"a\"", 2) + ",\"v\":" + env(4, "\"b\"", 2) + ",\"z\":" + env(4, "\"c\"", 2) + "}");
    assertEquals("a", doc.getString("r").getValue());
    assertEquals("b", doc.getString("v").getValue());
    assertEquals("c", doc.getString("z").getValue());
  }

  @Test
  public void rejectsPlainJson() {
    assertThrows(DataException.class, () -> convert("{\"name\":\"plain\"}"));
  }

  @Test
  public void rejectsEnvelopeWithExtraMember() {
    assertThrows(DataException.class, () -> convert("{\"a\":{\"r\":4,\"v\":\"x\",\"z\":2,\"extra\":1}}"));
  }

  @Test
  public void rejectsNonObject() {
    assertThrows(DataException.class, () -> convert("[1]"));
  }

  @Test
  public void rejectsMismatchedValue() {
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(4, "5", 2) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(3, "3000000000", 16) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(8, "\"nope\"", 7) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(1, "null", 99) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(3, "null", 16) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(7, "{\"l\":0,\"z\":\"\"}", 5) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(7, "\"abc\"", 200) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(11, "{\"t\":-1,\"z\":0}", 17) + "}"));
  }

  @Test
  public void rejectsRankThatDisagreesWithType() {
    // String (z=2) has rank 4
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(3, "\"x\"", 2) + "}"));
    // numeric types all have rank 3
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(4, "1", 16) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(4, "null", 201) + "}"));
    // null payloads are checked too: MinKey is rank 0, not 1
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(1, "null", 255) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(2, "null", 6) + "}"));
    // nested values are checked
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(6, "[" + env(3, "\"x\"", 2) + "]", 4) + "}"));
  }

  @Test
  public void rejectsEnvelopeTagsOutOfIntRange() {
    // 4294967297 is 2^32 + 1: truncated to an int it would read as type 1 (Double)
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(3, "1.5", 4294967297L) + "}"));
    // 4294967298 is 2^32 + 2: truncated to an int it would read as rank 2
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(4294967298L, "\"x\"", 2) + "}"));
    assertThrows(DataException.class, () -> convert("{\"a\":" + env(3, "1.5", -4294967295L) + "}"));
  }

  @Test
  public void rankIsCheckedForEveryTypeCode() {
    // one valid (rank, payload, type) per supported type code
    BsonDocument doc = convert("{" +
        "\"min\":" + env(0, "null", 255) + "," +
        "\"undef\":" + env(1, "null", 6) + "," +
        "\"null\":" + env(2, "null", 10) + "," +
        "\"dbl\":" + env(3, "1.5", 1) + "," +
        "\"nan\":" + env(3, "null", 201) + "," +
        "\"bin\":" + env(7, "{\"l\":0,\"s\":\"00\",\"z\":\"\"}", 5) + "," +
        "\"uuid\":" + env(7, "\"000102030405060708090a0b0c0d0e0f\"", 200) + "," +
        "\"ts\":" + env(11, "{\"t\":1,\"z\":2}", 17) + "," +
        "\"re\":" + env(12, "{\"p\":\"a\",\"z\":\"\"}", 11) + "," +
        "\"max\":" + env(13, "null", 127) +
        "}");
    assertEquals(10, doc.size());
  }

  @Test
  public void unservedTypesAreRejected() {
    // CWMDB can't store DBPointer (12), JavaScript (13), Symbol (14) or JavaScript with scope (15).
    for (int type : new int[]{12, 13, 14, 15}) {
      assertThrows(DataException.class, () -> convert("{\"a\":" + env(4, "\"x\"", type) + "}"), "type " + type);
    }
  }

  @Test
  public void rejectsNullCharacterInFieldName() {
    assertThrows(DataException.class, () -> convert("{\"a\\u0000b\":" + env(4, "\"x\"", 2) + "}"));
  }

  @Test
  public void rejectsNullCharacterInRegex() {
    assertThrows(DataException.class, () -> convert(
        "{\"re\":" + env(12, "{\"p\":\"a\\u0000b\",\"z\":\"\"}", 11) + "}"));
    assertThrows(DataException.class, () -> convert(
        "{\"re\":" + env(12, "{\"p\":\"a\",\"z\":\"i\\u0000\"}", 11) + "}"));
  }

  /**
   * Runs the handler against a mock event.
   *
   * @return the value of the resulting record, or {@code NO_RECORD} if the handler dropped the event
   */
  private static Object handle(DocumentEvent.Type type, byte[] content, boolean noValue) {
    DocumentEvent event = mock(DocumentEvent.class);
    when(event.type()).thenReturn(type);
    when(event.isMutation()).thenReturn(type == DocumentEvent.Type.MUTATION);
    when(event.content()).thenReturn(content);
    when(event.key()).thenReturn("k");
    when(event.qualifiedKey()).thenReturn("scope.collection.k");
    when(event.bucket()).thenReturn("bucket");

    SourceRecordBuilder builder = new BsonSourceHandler().handle(new SourceHandlerParams(event, "topic", noValue));
    if (builder == null) {
      return NO_RECORD;
    }

    return builder.build(mock(DocumentChange.class), Collections.emptyMap(), Collections.emptyMap(), "topic").value();
  }

  private static final Object NO_RECORD = new Object();

  private static byte[] bytes(String s) {
    return s.getBytes(UTF_8);
  }

  @Test
  public void mutationValueIsBson() {
    Object value = handle(DocumentEvent.Type.MUTATION, bytes("{\"a\":" + env(4, "\"x\"", 2) + "}"), false);

    BsonDocument doc = new BsonDocumentCodec().decode(
        new BsonBinaryReader(ByteBuffer.wrap((byte[]) value)),
        DecoderContext.builder().build());
    assertEquals("x", doc.getString("a").getValue());
  }

  @Test
  public void deletionAndExpirationHaveNullValue() {
    assertNull(handle(DocumentEvent.Type.DELETION, new byte[0], false));
    assertNull(handle(DocumentEvent.Type.EXPIRATION, new byte[0], false));
  }

  @Test
  public void noValueMutationHasNullValue() {
    // content is deliberately not an envelope document; it must not be read
    assertNull(handle(DocumentEvent.Type.MUTATION, bytes("not json"), true));
  }

  @Test
  public void nonJsonMutationIsDropped() {
    assertEquals(NO_RECORD, handle(DocumentEvent.Type.MUTATION, bytes("not json"), false));
    assertEquals(NO_RECORD, handle(DocumentEvent.Type.MUTATION, new byte[]{(byte) 0xff, 0x00, 0x01}, false));
  }

  @Test
  public void nonEnvelopeMutationIsDropped() {
    assertEquals(NO_RECORD, handle(DocumentEvent.Type.MUTATION, bytes("{\"name\":\"plain\"}"), false));
    assertEquals(NO_RECORD, handle(DocumentEvent.Type.MUTATION, bytes("[1,2]"), false));
  }

  @Test
  public void unconvertibleMutationIsDroppedRatherThanThrown() {
    // a null character in a field name is valid JSON but not valid BSON
    assertEquals(NO_RECORD, handle(DocumentEvent.Type.MUTATION,
        bytes("{\"a\\u0000b\":" + env(4, "\"x\"", 2) + "}"), false));
  }

  @Test
  public void handledRecordUsesDocumentIdAsKey() {
    DocumentEvent event = mock(DocumentEvent.class);
    when(event.type()).thenReturn(DocumentEvent.Type.DELETION);
    when(event.key()).thenReturn("the-key");

    SourceRecordBuilder builder = new BsonSourceHandler().handle(new SourceHandlerParams(event, "topic", false));
    assertNotNull(builder);
    assertEquals("the-key", builder.build(mock(DocumentChange.class), Collections.emptyMap(), Collections.emptyMap(), "topic").key());
  }

  @Test
  public void acceptsNestingAtCwmdbLimit() {
    final int maxNesting = 201;
    
    String json = env(4, "\"leaf\"", 2);
    for (int level = maxNesting - 1; level >= 1; level--) {
      json = (level % 2 == 0)
          ? env(6, "[" + json + "]", 4)
          : env(5, "{\"a\":" + json + "}", 3);
    }
    BsonDocument doc = convert("{\"a\":" + json + "}");

    // Walk down to the leaf, counting containers.
    int depth = 1; // the top-level document
    BsonValue value = doc.get("a");
    while (value.isDocument() || value.isArray()) {
      depth++;
      value = value.isDocument() ? value.asDocument().get("a") : value.asArray().get(0);
    }
    assertEquals(maxNesting, depth);
    assertEquals("leaf", value.asString().getValue());
  }
}
