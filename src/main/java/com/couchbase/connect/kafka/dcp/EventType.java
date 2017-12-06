/*
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

public enum EventType {
    MUTATION("mutation"),
    DELETION("deletion"),
    EXPIRATION("expiration"),
    SNAPSHOT("snapshot");

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
