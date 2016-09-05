/**
 * Copyright 2016 Couchbase, Inc.
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

package com.couchbase.connect.kafka.dcp;

import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;
import java.util.Map;

public enum EventType {
    MUTATION("mutation"),
    DELETION("deletion"),
    EXPIRATION("expiration"),
    SNAPSHOT("snapshot");

    public static final Map<EventType, Schema> SCHEMAS = new HashMap<EventType, Schema>();

    static {
        SchemaBuilder builder;

        builder = SchemaBuilder.struct().name(EventType.MUTATION.schemaName());
        builder.field("partition", org.apache.kafka.connect.data.Schema.INT16_SCHEMA);
        builder.field("key", org.apache.kafka.connect.data.Schema.STRING_SCHEMA);
        builder.field("expiration", org.apache.kafka.connect.data.Schema.INT32_SCHEMA);
        builder.field("flags", org.apache.kafka.connect.data.Schema.INT32_SCHEMA);
        builder.field("cas", org.apache.kafka.connect.data.Schema.INT64_SCHEMA);
        builder.field("lockTime", org.apache.kafka.connect.data.Schema.INT32_SCHEMA);
        builder.field("bySeqno", org.apache.kafka.connect.data.Schema.INT64_SCHEMA);
        builder.field("revSeqno", org.apache.kafka.connect.data.Schema.INT64_SCHEMA);
        builder.field("content", org.apache.kafka.connect.data.Schema.BYTES_SCHEMA);
        SCHEMAS.put(EventType.MUTATION, builder.build());

        builder = SchemaBuilder.struct().name(EventType.DELETION.schemaName());
        builder.field("partition", org.apache.kafka.connect.data.Schema.INT16_SCHEMA);
        builder.field("key", org.apache.kafka.connect.data.Schema.STRING_SCHEMA);
        builder.field("cas", org.apache.kafka.connect.data.Schema.INT64_SCHEMA);
        builder.field("bySeqno", org.apache.kafka.connect.data.Schema.INT64_SCHEMA);
        builder.field("revSeqno", org.apache.kafka.connect.data.Schema.INT64_SCHEMA);
        SCHEMAS.put(EventType.DELETION, builder.build());

        builder = SchemaBuilder.struct().name(EventType.EXPIRATION.schemaName());
        builder.field("partition", org.apache.kafka.connect.data.Schema.INT16_SCHEMA);
        builder.field("key", org.apache.kafka.connect.data.Schema.STRING_SCHEMA);
        builder.field("cas", org.apache.kafka.connect.data.Schema.INT64_SCHEMA);
        builder.field("bySeqno", org.apache.kafka.connect.data.Schema.INT64_SCHEMA);
        builder.field("revSeqno", org.apache.kafka.connect.data.Schema.INT64_SCHEMA);
        SCHEMAS.put(EventType.EXPIRATION, builder.build());
    }

    private final String schemaName;

    EventType(String schemaName) {
        this.schemaName = schemaName;
    }

    public static EventType of(ByteBuf message) {
        if (DcpMutationMessage.is(message)) {
            return EventType.MUTATION;
        } else if (DcpDeletionMessage.is(message)) {
            return EventType.DELETION;
        } else if (DcpExpirationMessage.is(message)) {
            return EventType.EXPIRATION;
        }
        return null;
    }

    public String schemaName() {
        return schemaName;
    }
}