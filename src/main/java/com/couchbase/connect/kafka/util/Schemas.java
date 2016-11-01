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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public enum Schemas {
    ;

    public static final Schema KEY_SCHEMA = SchemaBuilder.string().build();
    public static final Schema VALUE_DEFAULT_SCHEMA =
            SchemaBuilder.struct().name("com.couchbase.DcpMessage")
                    .field("event", Schema.STRING_SCHEMA)
                    .field("partition", Schema.INT16_SCHEMA)
                    .field("key", Schema.STRING_SCHEMA)
                    .field("cas", Schema.INT64_SCHEMA)
                    .field("bySeqno", Schema.INT64_SCHEMA)
                    .field("revSeqno", Schema.INT64_SCHEMA)
                    .field("expiration", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("flags", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("lockTime", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("content", Schema.OPTIONAL_BYTES_SCHEMA)
                    .build();
}
