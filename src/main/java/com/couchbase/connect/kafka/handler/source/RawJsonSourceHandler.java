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

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.connect.kafka.dcp.EventType;
import com.couchbase.connect.kafka.transform.DeserializeJson;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This handler propagates JSON documents from Couchbase to Kafka verbatim with no schema.
 * That is, the Kafka message will be identical to the content of the Couchbase document.
 * Deletions are propagated as a message with a {@code null} value.
 * Modifications to non-JSON documents are not propagated.
 * <p>
 * The key of the Kafka message is the ID of the Couchbase document.
 * <p>
 * The value of the generated ConnectRecord is a byte array.
 * If there are no downstream transforms, configure the connector like this
 * for maximum efficiency:
 * <pre>
 * dcp.message.converter.class=com.couchbase.connect.kafka.handler.source.RawJsonSourceHandler
 * value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
 * </pre>
 * If you wish to use Single Message Transforms with this handler, the first transform
 * must be {@link DeserializeJson} to convert the
 * byte array to a Map that downstream transforms can work with. Like this:
 * <pre>
 * dcp.message.converter.class=com.couchbase.connect.kafka.handler.source.RawJsonSourceHandler
 * value.converter=org.apache.kafka.connect.json.JsonConverter
 * value.converter.schemas.enable=false
 * transforms=deserializeJson,ignoreDeletes,addField
 * transforms.deserializeJson.type=com.couchbase.connect.kafka.transform.DeserializeJson
 * transforms.ignoreDeletes.type=com.couchbase.connect.kafka.transform.DropIfNullValue
 * transforms.addField.type=org.apache.kafka.connect.transforms.InsertField$Value
 * transforms.addField.static.field=magicWord
 * transforms.addField.static.value=xyzzy
 * </pre>
 *
 * @see RawJsonWithMetadataSourceHandler
 */
public class RawJsonSourceHandler extends SourceHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(RawJsonSourceHandler.class);

    private static final JsonFactory jsonFactory = new JsonFactory();

    protected static boolean isValidJson(byte[] bytes) {
        try {
            final JsonParser parser = jsonFactory.createParser(bytes);
            final JsonToken firstToken = parser.nextToken();

            final JsonToken incrementDepthToken;
            final JsonToken decrementDepthToken;

            if (firstToken == JsonToken.START_OBJECT) {
                incrementDepthToken = JsonToken.START_OBJECT;
                decrementDepthToken=JsonToken.END_OBJECT;

            } else if (firstToken == JsonToken.START_ARRAY) {
                incrementDepthToken = JsonToken.START_ARRAY;
                decrementDepthToken=JsonToken.END_ARRAY;

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

    @Override
    public CouchbaseSourceRecord handle(SourceHandlerParams params) {
        final CouchbaseSourceRecord.Builder builder = CouchbaseSourceRecord.builder();

        if (!passesFilter(params)) {
            return null;
        }

        if (!buildValue(params, builder)) {
            return null;
        }

        return builder.topic(getTopic(params))
                .key(Schema.STRING_SCHEMA, params.documentEvent().key())
                .build();
    }

    protected boolean passesFilter(SourceHandlerParams params) {
        return true;
    }

    protected boolean buildValue(SourceHandlerParams params, CouchbaseSourceRecord.Builder builder) {
        final DocumentEvent docEvent = params.documentEvent();
        final ByteBuf event = docEvent.rawDcpEvent();

        final EventType type = EventType.of(event);
        if (type == null) {
            LOGGER.warn("unexpected event type {}", event.getByte(1));
            return false;
        }

        switch (type) {
            case EXPIRATION:
            case DELETION:
                builder.value(null, null);
                return true;

            case MUTATION:
                final byte[] document = MessageUtil.getContentAsByteArray(event);
                if (!isValidJson(document)) {
                    LOGGER.warn("Skipping non-JSON document: bucket={} key={}", docEvent.bucket(), docEvent.key());
                    return false;
                }

                builder.value(null, document);
                return true;

            default:
                LOGGER.warn("unexpected event type {}", event.getByte(1));
                return false;
        }
    }

    protected String getTopic(SourceHandlerParams params) {
        // Alter the topic based on document key / content:
        //
        // if (params.documentEvent().key().startsWith("xyzzy")) {
        //     return params.topic() + "-xyzzy";
        // }

        // Or use the default topic
        return null;
    }
}
