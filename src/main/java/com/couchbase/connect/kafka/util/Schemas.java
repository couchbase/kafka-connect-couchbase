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

package com.couchbase.connect.kafka.util;

import com.couchbase.connect.kafka.dcp.EventType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;
import java.util.Map;

public enum Schemas {
    ;

    public static final Schema KEY_SCHEMA = SchemaBuilder.string().build();
    public static final Map<EventType, Schema> VALUE_SCHEMAS = new HashMap<EventType, Schema>();

    static {
        SchemaBuilder builder;

        builder = SchemaBuilder.struct().name(EventType.MUTATION.schemaName());
        builder.field("partition", Schema.INT16_SCHEMA);
        builder.field("key", Schema.STRING_SCHEMA);
        builder.field("expiration", Schema.INT32_SCHEMA);
        builder.field("flags", Schema.INT32_SCHEMA);
        builder.field("cas", Schema.INT64_SCHEMA);
        builder.field("lockTime", Schema.INT32_SCHEMA);
        builder.field("bySeqno", Schema.INT64_SCHEMA);
        builder.field("revSeqno", Schema.INT64_SCHEMA);
        builder.field("content", Schema.BYTES_SCHEMA);
        VALUE_SCHEMAS.put(EventType.MUTATION, builder.build());

        builder = SchemaBuilder.struct().name(EventType.DELETION.schemaName());
        builder.field("partition", Schema.INT16_SCHEMA);
        builder.field("key", Schema.STRING_SCHEMA);
        builder.field("cas", Schema.INT64_SCHEMA);
        builder.field("bySeqno", Schema.INT64_SCHEMA);
        builder.field("revSeqno", Schema.INT64_SCHEMA);
        VALUE_SCHEMAS.put(EventType.DELETION, builder.build());

        builder = SchemaBuilder.struct().name(EventType.EXPIRATION.schemaName());
        builder.field("partition", Schema.INT16_SCHEMA);
        builder.field("key", Schema.STRING_SCHEMA);
        builder.field("cas", Schema.INT64_SCHEMA);
        builder.field("bySeqno", Schema.INT64_SCHEMA);
        builder.field("revSeqno", Schema.INT64_SCHEMA);
        VALUE_SCHEMAS.put(EventType.EXPIRATION, builder.build());
    }


    static {


    }
}
