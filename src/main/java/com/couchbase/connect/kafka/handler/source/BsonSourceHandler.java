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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonFactory;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.StreamReadConstraints;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonBinaryWriter;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonWriter;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

/**
 * This handler propagates documents from a Couchbase with MongoDB Compatibility
 * (CWMDB) collection to Kafka as BSON.
 * <p>
 * Deletions and expirations are propagated as a message with a {@code null} value.
 * Documents that are not in the CWMDB envelope format, or that cannot be
 * represented as BSON, are not propagated;
 * a warning is logged instead.
 * <p>
 * The key of the Kafka message is the ID of the Couchbase document, not the interior _id of the contents
 * <p>
 * To source a collection in Couchbase with MongoDB compatibility, set {@code couchbase.scope} to the MongoDB
 * database name and {@code couchbase.collections} to the collection name,
 * and configure the connector like this:
 * <pre>
 * couchbase.source.handler=com.couchbase.connect.kafka.handler.source.BsonSourceHandler
 * value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
 * </pre>
 *
 * @see RawJsonSourceHandler
 */
public class BsonSourceHandler extends RawJsonSourceHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(BsonSourceHandler.class);

  // Jackson's default 20M character limit on a single string would reject a document with a
  // binary over about 15 MB (base64 inflates it by a third), even though MongoDB allows
  // documents up to 16 MiB. Dropping it would silently lose the document.
  private static final ObjectMapper objectMapper = new ObjectMapper(JsonFactory.builder()
      .streamReadConstraints(StreamReadConstraints.builder()
          .maxStringLength(Integer.MAX_VALUE)
          .build())
      .build());

  private static final int SENTINEL_ERROR = -1;
  private static final int DOUBLE = 1;
  private static final int STRING = 2;
  private static final int DOCUMENT = 3;
  private static final int ARRAY = 4;
  private static final int BINARY = 5;
  private static final int UNDEFINED = 6;
  private static final int OBJECT_ID = 7;
  private static final int BOOLEAN = 8;
  private static final int DATE = 9;
  private static final int NULL = 10;
  private static final int REGEX = 11;
  private static final int INT32 = 16;
  private static final int TIMESTAMP = 17;
  private static final int INT64 = 18;
  private static final int DECIMAL128 = 19;
  private static final int MAX_KEY = 127;
  private static final int UUID_HEX_STRING = 200;
  private static final int DOUBLE_NAN = 201;
  private static final int DOUBLE_NEG_INFINITY = 202;
  private static final int DOUBLE_POS_INFINITY = 203;
  private static final int MIN_KEY = 255;

  private static final int UNKNOWN_RANK = -1;

  @Override
  protected boolean buildValue(SourceHandlerParams params, SourceRecordBuilder builder) {
    final DocumentEvent docEvent = params.documentEvent();

    // Pass up to logic in the RawJsonSourceHandler
    if (!docEvent.isMutation() || params.noValue()) {
      return super.buildValue(params, builder);
    }

    try {
      builder.value(null, jsonEnvelopeToBson(docEvent.content()));
      return true;
    } catch (DataException e) {
      LOGGER.warn("Skipping document that could not be converted to BSON: bucket={} key={} reason={}",
          docEvent.bucket(), docEvent.qualifiedKey(), e.getMessage());
      return false;
    }
  }

  /**
   * Converts a CWMDB document (JSON with {r, v, z} field envelopes) to a BSON document.
   *
   * @throws DataException if the content is not a well-formed CWMDB document,
   * or cannot be represented as BSON
   */
  static byte[] jsonEnvelopeToBson(byte[] json) {
    final JsonNode root;
    try {
      root = objectMapper.readTree(json);
    } catch (IOException e) {
      throw new DataException("Content is not valid JSON.", e);
    }

    if (root == null || !root.isObject()) {
      throw new DataException("Expected top-level JSON object.");
    }

    try (BasicOutputBuffer buffer = new BasicOutputBuffer(json.length);
         BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
      writeDocumentFields(root, writer, "");
      writer.flush();
      return buffer.toByteArray();

    } catch (DataException e) {
      throw e;
    } catch (RuntimeException e) {
      // The BSON writer rejects content that is valid JSON but not valid BSON,
      // such as a field name or regex pattern containing a null character.
      throw new DataException("Failed to convert document to BSON: " + e.getMessage(), e);
    }
  }

  /**
   * A valid CWMDB envelope has exactly the members r, v, and z
   */
  private static boolean isEnvelope(JsonNode node) {
    return node.isObject()
        && node.size() == 3
        && node.has("v")
        && node.path("r").isIntegralNumber()
        && node.path("z").isIntegralNumber();
  }

  private static void writeDocumentFields(JsonNode fields, BsonWriter writer, String path) {
    writer.writeStartDocument();
    for (Map.Entry<String, JsonNode> field : fields.properties()) {
      writer.writeName(field.getKey());
      writeEnvelope(path.isEmpty() ? field.getKey() : path + "." + field.getKey(), field.getValue(), writer);
    }
    writer.writeEndDocument();
  }

  /**
   * Write the converted BSON value of the envelope to the BsonWriter
   */
  private static void writeEnvelope(String path, JsonNode envelope, BsonWriter writer) {
    if (!isEnvelope(envelope)) {
      throw new DataException("Value of '" + path + "' is not a {r,v,z} envelope.");
    }
    
    final int type = requireInt(path + ".z", envelope.get("z"));
    final int rank = requireInt(path + ".r", envelope.get("r"));
    final JsonNode value = envelope.get("v");

    // The rank is a function of the type code, so a disagreement means the
    // document is malformed. Neither member is trusted over the other.
    final int expectedRank = rankOf(type);
    if (expectedRank != UNKNOWN_RANK && rank != expectedRank) {
      throw new DataException("Envelope of '" + path + "' has rank " + rank
          + " but type " + type + " requires rank " + expectedRank + ".");
    }

    if (value.isNull()) {
      writeNullPayload(path, type, writer);
      return;
    }

    switch (type) {
      case SENTINEL_ERROR:
        throw new DataException("Stored error marker at '" + path + "': " + value.asText());
      case DOUBLE:
        if (!value.isNumber()) {
          throw typeMismatch(path, "number");
        }
        writer.writeDouble(value.doubleValue());
        break;
      case STRING:
        writer.writeString(requireText(path, value));
        break;
      case DOCUMENT:
        if (!value.isObject()) {
          throw typeMismatch(path, "object");
        }
        writeDocumentFields(value, writer, path);
        break;
      case ARRAY:
        if (!value.isArray()) {
          throw typeMismatch(path, "array");
        }
        writer.writeStartArray();
        for (int i = 0; i < value.size(); i++) {
          writeEnvelope(path + "." + i, value.get(i), writer);
        }
        writer.writeEndArray();
        break;
      case BINARY:
        writeBinary(path, value, writer);
        break;
      case UUID_HEX_STRING:
        // Drivers reject a subtype 4 binary that isn't 16 bytes when decoding it as a UUID.
        final String uuidHex = requireText(path, value);
        if (uuidHex.length() != 32) {
          throw new DataException("Value of '" + path + "' is not a 32-character UUID hex string.");
        }
        writer.writeBinaryData(new BsonBinary(BsonBinarySubType.UUID_STANDARD, hexToBytes(path, uuidHex)));
        break;
      case OBJECT_ID:
        try {
          writer.writeObjectId(new ObjectId(requireText(path, value)));
        } catch (IllegalArgumentException e) {
          throw new DataException("Value of '" + path + "' is not a valid ObjectId.", e);
        }
        break;
      case BOOLEAN:
        if (!value.isBoolean()) {
          throw typeMismatch(path, "boolean");
        }
        writer.writeBoolean(value.booleanValue());
        break;
      case DATE:
        writer.writeDateTime(toLong(path, value));
        break;
      case REGEX:
        writer.writeRegularExpression(new BsonRegularExpression(
            requireText(path + ".p", value.path("p")),
            requireText(path + ".z", value.path("z"))));
        break;
      case INT32:
        long i32 = toLong(path, value);
        if (i32 < Integer.MIN_VALUE || i32 > Integer.MAX_VALUE) {
          throw new DataException("Value of '" + path + "' is out of range for Int32.");
        }
        writer.writeInt32((int) i32);
        break;
      case TIMESTAMP:
        writer.writeTimestamp(new BsonTimestamp(
            toUnsignedInt(path + ".t", value.path("t")),
            toUnsignedInt(path + ".z", value.path("z"))));
        break;
      case INT64:
        writer.writeInt64(toLong(path, value));
        break;
      case DECIMAL128:
        try {
          writer.writeDecimal128(Decimal128.parse(requireText(path, value)));
        } catch (NumberFormatException e) {
          throw new DataException("Value of '" + path + "' is not a valid Decimal128.", e);
        }
        break;
      default:
        throw new DataException("Unsupported BSON type code " + type + " at '" + path + "'.");
    }
  }
  
  private static int rankOf(int type) {
    switch (type) {
      case MIN_KEY:
        return 0;
      case UNDEFINED:
        return 1;
      case NULL:
        return 2;
      case DOUBLE:
      case INT32:
      case INT64:
      case DECIMAL128:
      case DOUBLE_NAN:
      case DOUBLE_NEG_INFINITY:
      case DOUBLE_POS_INFINITY:
        return 3;
      case STRING:
        return 4;
      case DOCUMENT:
        return 5;
      case ARRAY:
        return 6;
      case BINARY:
      case UUID_HEX_STRING:
        return 7;
      case OBJECT_ID:
        return 8;
      case BOOLEAN:
        return 9;
      case DATE:
        return 10;
      case TIMESTAMP:
        return 11;
      case REGEX:
        return 12;
      case MAX_KEY:
        return 13;
      case SENTINEL_ERROR:
        return 99;
      default:
        return UNKNOWN_RANK;
    }
  }

  /**
   * Writes the value a null payload stands for. Only the type code can say which.
   */
  private static void writeNullPayload(String path, int type, BsonWriter writer) {
    switch (type) {
      case NULL:
        writer.writeNull();
        break;
      case UNDEFINED:
        writer.writeUndefined();
        break;
      case MIN_KEY:
        writer.writeMinKey();
        break;
      case MAX_KEY:
        writer.writeMaxKey();
        break;
      case DOUBLE_NAN:
        writer.writeDouble(Double.NaN);
        break;
      case DOUBLE_NEG_INFINITY:
        writer.writeDouble(Double.NEGATIVE_INFINITY);
        break;
      case DOUBLE_POS_INFINITY:
        writer.writeDouble(Double.POSITIVE_INFINITY);
        break;
      default:
        throw new DataException("Null payload for unexpected type code " + type + " at '" + path + "'.");
    }
  }

  private static void writeBinary(String path, JsonNode value, BsonWriter writer) {
    if (!value.isObject()) {
      throw typeMismatch(path, "object");
    }

    final String subtypeHex = requireText(path + ".s", value.path("s"));
    final int subtype;
    try {
      subtype = Integer.parseInt(subtypeHex, 16);
    } catch (NumberFormatException e) {
      throw new DataException("Binary subtype of '" + path + "' is not hex.", e);
    }
    if (subtype < 0 || subtype > 0xFF) {
      throw new DataException("Binary subtype of '" + path + "' is out of range.");
    }

    final byte[] data;
    try {
      data = Base64.getDecoder().decode(requireText(path + ".z", value.path("z")));
    } catch (IllegalArgumentException e) {
      throw new DataException("Value of '" + path + "' is not valid base64.", e);
    }
    writer.writeBinaryData(new BsonBinary((byte) subtype, withoutOldBinaryLengthPrefix(path, subtype, data)));
  }

  /**
   * The deprecated "old binary" subtype (2) wraps its data in a second length prefix.
   * CWMDB stores the bytes exactly as they appear on the wire, prefix included,
   * but the BSON writer adds the prefix itself, so it has to be removed here
   * or the output would have it twice.
   */
  private static byte[] withoutOldBinaryLengthPrefix(String path, int subtype, byte[] data) {
    if (subtype != BsonBinarySubType.OLD_BINARY.getValue()) {
      return data;
    }

    final int prefix = Integer.BYTES;
    if (data.length < prefix
        || ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt() != data.length - prefix) {
      throw new DataException("Binary subtype 2 value of '" + path + "' has an invalid length prefix.");
    }
    return Arrays.copyOfRange(data, prefix, data.length);
  }

  private static byte[] hexToBytes(String path, String hex) {
    if (hex.length() % 2 != 0) {
      throw new DataException("Value of '" + path + "' is not valid hex.");
    }
    final byte[] bytes = new byte[hex.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      int hi = Character.digit(hex.charAt(2 * i), 16);
      int lo = Character.digit(hex.charAt(2 * i + 1), 16);
      if (hi < 0 || lo < 0) {
        throw new DataException("Value of '" + path + "' is not valid hex.");
      }
      bytes[i] = (byte) (hi << 4 | lo);
    }
    return bytes;
  }

  private static String requireText(String path, JsonNode value) {
    if (!value.isTextual()) {
      throw typeMismatch(path, "string");
    }
    return value.textValue();
  }

  private static int requireInt(String path, JsonNode value) {
    if (!value.canConvertToInt()) {
      throw new DataException("Envelope member '" + path + "' is out of range: " + value);
    }
    return value.intValue();
  }

  private static long toLong(String path, JsonNode value) {
    if (value.isIntegralNumber() && value.canConvertToLong()) {
      return value.longValue();
    }
    throw typeMismatch(path, "integer");
  }

  /**
   * Reads an unsigned 32-bit integer. The bits are returned as an int,
   * which is how {@link BsonTimestamp} takes them.
   */
  private static int toUnsignedInt(String path, JsonNode value) {
    long n = toLong(path, value);
    if (n < 0 || n > 0xFFFFFFFFL) {
      throw new DataException("Value of '" + path + "' is out of range for an unsigned 32-bit integer.");
    }
    return (int) n;
  }

  private static DataException typeMismatch(String path, String expected) {
    return new DataException("Value of '" + path + "' is not the expected " + expected + ".");
  }
}
