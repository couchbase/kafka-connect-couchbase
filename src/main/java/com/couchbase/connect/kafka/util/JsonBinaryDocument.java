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

package com.couchbase.connect.kafka.util;

import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.document.AbstractDocument;

/**
 * A JSON document backed by a ByteBuf. Unlike RawJsonDocument,
 * does not require copying the bytes into String.
 */
public class JsonBinaryDocument extends AbstractDocument<ByteBuf> {
    public static JsonBinaryDocument create(String id, int expiry, ByteBuf content) {
        return new JsonBinaryDocument(id, expiry, content, 0L, null);
    }

    public static JsonBinaryDocument create(String id, byte[] content) {
        return create(id, 0, content);
    }

    public static JsonBinaryDocument create(String id, int expiry, byte[] content) {
        return create(id, expiry, content == null ? null : Unpooled.wrappedBuffer(content));
    }

    public JsonBinaryDocument(String id, int expiry, ByteBuf content, long cas, MutationToken mutationToken) {
        super(id, expiry, content, cas, mutationToken);
    }
}
