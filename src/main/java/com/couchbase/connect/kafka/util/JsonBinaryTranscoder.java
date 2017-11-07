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

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.transcoder.AbstractTranscoder;
import com.couchbase.client.java.transcoder.TranscoderUtils;

/**
 * Like BinaryTranscoder, but flags content as JSON and only supports encoding
 * (because that's all the Kafka sink connector needs).
 */
public class JsonBinaryTranscoder extends AbstractTranscoder<JsonBinaryDocument, ByteBuf> {
    protected JsonBinaryDocument doDecode(String id, ByteBuf content, long cas, int expiry, int flags, ResponseStatus status) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected Tuple2<ByteBuf, Integer> doEncode(JsonBinaryDocument document) throws Exception {
        return Tuple.create(document.content(), TranscoderUtils.JSON_COMPAT_FLAGS);
    }

    public JsonBinaryDocument newDocument(String id, int expiry, ByteBuf content, long cas) {
        return new JsonBinaryDocument(id, expiry, content, cas, null);
    }

    public JsonBinaryDocument newDocument(String id, int expiry, ByteBuf content, long cas, MutationToken mutationToken) {
        return new JsonBinaryDocument(id, expiry, content, cas, mutationToken);
    }

    public Class<JsonBinaryDocument> documentType() {
        return JsonBinaryDocument.class;
    }
}
