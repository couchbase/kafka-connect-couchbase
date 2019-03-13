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

import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.connect.kafka.dcp.EventType;
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
 * dcp.message.converter.class=com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler
 * value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
 * </pre>
 *
 * @see RawJsonSourceHandler
 */
public class RawJsonWithMetadataSourceHandler extends RawJsonSourceHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RawJsonWithMetadataSourceHandler.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CouchbaseSourceRecord handle(SourceHandlerParams params) {
        CouchbaseSourceRecord.Builder builder = CouchbaseSourceRecord.builder();

        if (!buildValue(params, builder)) {
            return null;
        }

        return builder
                .topic(getTopic(params))
                .key(Schema.STRING_SCHEMA, params.documentEvent().key())
                .build();
    }

    protected boolean buildValue(SourceHandlerParams params, CouchbaseSourceRecord.Builder builder) {

        if (!super.buildValue(params, builder)) {
            return false;
        }

        final DocumentEvent docEvent = params.documentEvent();
        final ByteBuf event = docEvent.rawDcpEvent();

        final EventType type = EventType.of(event);

        Map<String, Object> metadata = new HashMap<String, Object>();

        metadata.put("bucket", docEvent.bucket());
        metadata.put("partition", docEvent.vBucket());
        metadata.put("vBucketUuid", docEvent.vBucketUuid());
        metadata.put("key", docEvent.key());
        metadata.put("cas", docEvent.cas());
        metadata.put("bySeqno", docEvent.bySeqno());
        metadata.put("revSeqno", docEvent.revisionSeqno());

        if (type == EventType.MUTATION) {
            metadata.put("event", "mutation");
            metadata.put("expiration", DcpMutationMessage.expiry(event));
            metadata.put("flags", DcpMutationMessage.flags(event));
            metadata.put("lockTime", DcpMutationMessage.lockTime(event));

        } else if (type == EventType.DELETION) {
            metadata.put("event", "deletion");
        } else if (type == EventType.EXPIRATION) {
            metadata.put("event", "expiration");
        } else {
            LOGGER.warn("unexpected event type {}", event.getByte(1));
            return false;
        }

        try {
            byte[] value = objectMapper.writeValueAsBytes(metadata);
            if (type == EventType.MUTATION) {
                value = withContentField(value, (byte[]) builder.value());
            }
            builder.value(null, value);
            return true;
        } catch (JsonProcessingException e) {
            throw new DataException("Failed to serialize event metadata", e);
        }
    }

    private static final byte[] contentFieldNameBytes = ",\"content\":".getBytes(UTF_8);

    protected static byte[] withContentField(byte[] metadata, byte[] documentContent) {
        final int resultLength = metadata.length + contentFieldNameBytes.length + documentContent.length;
        return new ByteArrayBuilder(resultLength)
                .append(metadata, metadata.length - 1) // omit trailing brace; we'll add it later
                .append(contentFieldNameBytes) // ,content=
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
