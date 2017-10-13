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

import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.connect.kafka.converter.ConverterUtils.bufToString;

/**
 * A Couchbase document change event.
 */
public class DocumentEvent {
    private final ByteBuf rawDcpEvent;
    private final String bucket;
    private final long vBucketUuid;
    private final long bySeqno;
    private final long revisionSeqno;
    private final String key;

    public static DocumentEvent create(ByteBuf rawDcpEvent, String bucket, long vBucketUuid) {
        return new DocumentEvent(rawDcpEvent, bucket, vBucketUuid);
    }

    private DocumentEvent(ByteBuf rawDcpEvent, String bucket, long vBucketUuid) {
        this.rawDcpEvent = rawDcpEvent;
        this.bucket = bucket;
        this.vBucketUuid = vBucketUuid;
        this.key = bufToString(MessageUtil.getKey(rawDcpEvent));

        if (DcpMutationMessage.is(rawDcpEvent)) {
            this.bySeqno = DcpMutationMessage.bySeqno(rawDcpEvent);
            this.revisionSeqno = DcpMutationMessage.revisionSeqno(rawDcpEvent);
        } else if (DcpDeletionMessage.is(rawDcpEvent)) {
            this.bySeqno = DcpDeletionMessage.bySeqno(rawDcpEvent);
            this.revisionSeqno = DcpDeletionMessage.revisionSeqno(rawDcpEvent);
        } else if (DcpExpirationMessage.is(rawDcpEvent)) {
            this.bySeqno = DcpExpirationMessage.bySeqno(rawDcpEvent);
            this.revisionSeqno = DcpExpirationMessage.revisionSeqno(rawDcpEvent);
        } else {
            this.bySeqno = 0;
            this.revisionSeqno = 0;
        }
    }

    public ByteBuf rawDcpEvent() {
        return rawDcpEvent;
    }

    public String bucket() {
        return bucket;
    }

    public short vBucket() {
        return MessageUtil.getVbucket(rawDcpEvent);
    }

    public long vBucketUuid() {
        return vBucketUuid;
    }

    public String key() {
        return key;
    }

    public long cas() {
        return MessageUtil.getCas(rawDcpEvent);
    }

    public long bySeqno() {
        return bySeqno;
    }

    public long revisionSeqno() {
        return revisionSeqno;
    }
}
