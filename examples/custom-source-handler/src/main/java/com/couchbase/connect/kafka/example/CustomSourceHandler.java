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

package com.couchbase.connect.kafka.example;

import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.ByteBufInputStream;
import com.couchbase.connect.kafka.dcp.EventType;
import com.couchbase.connect.kafka.handler.source.CouchbaseSourceRecord;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.SourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomSourceHandler extends SourceHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomSourceHandler.class);

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

    private boolean buildValue(SourceHandlerParams params, CouchbaseSourceRecord.Builder builder) {
        final DocumentEvent docEvent = params.documentEvent();
        final ByteBuf event = docEvent.rawDcpEvent();

        final EventType type = EventType.of(event);
        if (type == null) {
            LOGGER.warn("unexpected event type {}", event.getByte(1));
            return false;
        }

        Map<String, Object> record = new HashMap<String, Object>();

        record.put("bucket", docEvent.bucket());
        record.put("partition", docEvent.vBucket());
        record.put("vBucketUuid", docEvent.vBucketUuid());
        record.put("key", docEvent.key());
        record.put("cas", docEvent.cas());
        record.put("bySeqno", docEvent.bySeqno());
        record.put("revSeqno", docEvent.revisionSeqno());

        if (type == EventType.MUTATION) {
            record.put("event", "mutation");
            record.put("expiration", DcpMutationMessage.expiry(event));
            record.put("flags", DcpMutationMessage.flags(event));
            record.put("lockTime", DcpMutationMessage.lockTime(event));

            try {
                record.put("content", objectMapper.readValue(
                        new ByteBufInputStream(DcpMutationMessage.content(event)), Map.class));

            } catch (IOException e) {
                LOGGER.warn("Skipping non-JSON document: bucket={} key={}", docEvent.bucket(), docEvent.key(), e);
                return false;
            }

        } else if (type == EventType.DELETION) {
            record.put("event", "deletion");
        } else if (type == EventType.EXPIRATION) {
            record.put("event", "expiration");
        } else {
            LOGGER.warn("unexpected event type {}", event.getByte(1));
            return false;
        }

        // For this example the value is a schema-less Map. When `value.converter` is set to JsonConverter,
        // the Kafka message body will consist of the JSON-serialized form of the map
        // (assuming it contains only [boxed] primitives, otherwise conversion will fail).
        builder.value(null, record);
        return true;

        // Alternatively, you could do the JSON serialization yourself, pass a String value,
        // and set `value.converter` to `org.apache.kafka.connect.storage.StringConverter`
        // (which would take the JSON string and convert it to a String (which is effectively a no-op).
        //
        // If you want to take this a step further, you can do your own serialization to a byte array
        // and write a custom implementation of `org.apache.kafka.connect.storage.Converter`
        // that just passes the byte array along unmodified.
    }

    private String getTopic(SourceHandlerParams params) {
        // Alter the topic based on document key / content:
        //
        // if (params.documentEvent().key().startsWith("xyzzy")) {
        //     return params.topic() + "-xyzzy";
        // }

        // Or use the default topic
        return null;
    }
}
