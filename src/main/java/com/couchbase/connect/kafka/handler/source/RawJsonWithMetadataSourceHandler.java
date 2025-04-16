/*
 * Copyright 2017 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This handler includes Couchbase metadata when propagating JSON documents
 * from Couchbase to Kafka. Deletions and expirations are propagated as a JSON
 * document whose "event" field is "deletion" or "expiration". Mutations are
 * propagated with "event" field value of "mutation", with Couchbase document
 * body in the "content" field. Modifications to non-JSON documents are not
 * propagated.
 * <p>
 * The key of the Kafka message is a String, the ID of the Couchbase document.
 * <p>
 * To use this handler, configure the connector properties like this:
 * <pre>
 * couchbase.source.handler=com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler
 * value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
 * </pre>
 *
 * @see RawJsonSourceHandler
 */
public class RawJsonWithMetadataSourceHandler extends RawJsonSourceHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawJsonWithMetadataSourceHandler.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public SourceRecordBuilder handle(SourceHandlerParams params) {
    SourceRecordBuilder builder = new SourceRecordBuilder();

    if (!buildValue(params, builder)) {
      return null;
    }

    return builder
        .topic(getTopic(params))
        .key(Schema.STRING_SCHEMA, params.documentEvent().key());
  }

  protected boolean buildValue(SourceHandlerParams params, SourceRecordBuilder builder) {
    if (!super.buildValue(params, builder)) {
      return false;
    }

    final DocumentEvent docEvent = params.documentEvent();
    final DocumentEvent.Type type = docEvent.type();

    Map<String, Object> metadata = new HashMap<>();
    metadata.put("event", type.schemaName());

    metadata.put("bucket", docEvent.bucket());
    metadata.put("partition", docEvent.partition());
    metadata.put("vBucketUuid", docEvent.partitionUuid());
    metadata.put("key", docEvent.key());
    metadata.put("cas", docEvent.cas());
    metadata.put("bySeqno", docEvent.bySeqno());
    metadata.put("revSeqno", docEvent.revisionSeqno());

    final MutationMetadata mutation = docEvent.mutationMetadata().orElse(null);
    if (mutation != null) {
      metadata.put("expiration", mutation.expiry());
      metadata.put("flags", mutation.flags());
      metadata.put("lockTime", mutation.lockTime());

    } else if (type != DocumentEvent.Type.DELETION && type != DocumentEvent.Type.EXPIRATION) {
      LOGGER.warn("unexpected event type");
      return false;
    }

    customizeMetadata(docEvent, metadata);

    try {
      byte[] value = objectMapper.writeValueAsBytes(metadata);
      if (docEvent.isMutation() && !params.noValue()) {
        value = withContentField(value, docEvent.content());
      }
      builder.value(null, value);
      return true;
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to serialize event metadata", e);
    }
  }

  /**
   * Customizes the metadata based on the given document event.
   * <p>
   * This method is intended to be overridden in subclasses to modify or add metadata
   * when a document event occurs. The metadata map allows storing key-value pairs
   * related to the document event.
   *
   * @param docEvent The document event that triggers the metadata customization.
   * It provides context about the change occurring in the document.
   * @param metadata A map containing the existing metadata related to the document.
   * Implementations may modify this map to add or update its entries.
   */
  protected void customizeMetadata(final DocumentEvent docEvent, final Map<String, Object> metadata) {
  }

  private static final byte[] contentFieldNameBytes = ",\"content\":".getBytes(UTF_8);

  protected static byte[] withContentField(byte[] metadata, byte[] documentContent) {
    final int resultLength = metadata.length + contentFieldNameBytes.length + documentContent.length;
    return new ByteArrayBuilder(resultLength)
        .append(metadata, metadata.length - 1) // omit trailing brace; we'll add it later
        .append(contentFieldNameBytes) // ,"content":
        .append(documentContent) // known to be well-formed JSON
        .append((byte) '}')
        .build();
  }

  // A zero-copy cousin of ByteArrayOutputStream
  protected static class ByteArrayBuilder {
    private final byte[] bytes;
    private int destIndex = 0;

    public ByteArrayBuilder(int finalSize) {
      this.bytes = new byte[finalSize];
    }

    public ByteArrayBuilder append(byte[] source, int len) {
      System.arraycopy(source, 0, bytes, destIndex, len);
      destIndex += len;
      return this;
    }

    public ByteArrayBuilder append(byte[] source) {
      return append(source, source.length);
    }

    public ByteArrayBuilder append(byte b) {
      bytes[destIndex++] = b;
      return this;
    }

    public byte[] build() {
      if (destIndex != bytes.length) {
        throw new IllegalStateException("Byte array not sized properly. Expected " + bytes.length + " bytes but got " + destIndex);
      }
      return bytes;
    }
  }
}
