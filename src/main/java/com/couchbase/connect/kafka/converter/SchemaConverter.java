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

package com.couchbase.connect.kafka.converter;

import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.connect.kafka.dcp.EventType;
import com.couchbase.connect.kafka.handler.source.DefaultSchemaSourceHandler;
import com.couchbase.connect.kafka.util.Schemas;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.couchbase.connect.kafka.converter.ConverterUtils.bufToBytes;
import static com.couchbase.connect.kafka.converter.ConverterUtils.bufToString;

/**
 * @deprecated in favor of {@link DefaultSchemaSourceHandler}.
 * Scheduled for removal in 4.0.0.
 */
@Deprecated
public class SchemaConverter implements Converter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaConverter.class);

    @Override
    public SourceRecord convert(final ByteBuf event, final String bucket, final String topic) {
        EventType type = EventType.of(event);
        if (type != null) {
            Struct record = new Struct(Schemas.VALUE_DEFAULT_SCHEMA);
            String key;
            long seqno;
            if (DcpMutationMessage.is(event)) {
                key = bufToString(DcpMutationMessage.key(event));
                seqno = DcpMutationMessage.bySeqno(event);
                record.put("event", "mutation");
                record.put("partition", DcpMutationMessage.partition(event));
                record.put("key", key);
                record.put("expiration", DcpMutationMessage.expiry(event));
                record.put("flags", DcpMutationMessage.flags(event));
                record.put("cas", DcpMutationMessage.cas(event));
                record.put("lockTime", DcpMutationMessage.lockTime(event));
                record.put("bySeqno", seqno);
                record.put("revSeqno", DcpMutationMessage.revisionSeqno(event));
                record.put("content", bufToBytes(DcpMutationMessage.content(event)));
            } else if (DcpDeletionMessage.is(event)) {
                key = bufToString(DcpDeletionMessage.key(event));
                seqno = DcpDeletionMessage.bySeqno(event);
                record.put("event", "deletion");
                record.put("partition", DcpDeletionMessage.partition(event));
                record.put("key", key);
                record.put("cas", DcpDeletionMessage.cas(event));
                record.put("bySeqno", seqno);
                record.put("revSeqno", DcpDeletionMessage.revisionSeqno(event));
            } else if (DcpExpirationMessage.is(event)) {
                key = bufToString(DcpExpirationMessage.key(event));
                seqno = DcpExpirationMessage.bySeqno(event);
                record.put("event", "expiration");
                record.put("partition", DcpExpirationMessage.partition(event));
                record.put("key", key);
                record.put("cas", DcpExpirationMessage.cas(event));
                record.put("bySeqno", seqno);
                record.put("revSeqno", DcpExpirationMessage.revisionSeqno(event));
            } else {
                LOGGER.warn("unexpected event type {}", event.getByte(1));
                return null;
            }
            final Map<String, Object> offset = new HashMap<String, Object>(2);
            offset.put("bySeqno", seqno);
            final Map<String, String> partition = new HashMap<String, String>(2);
            partition.put("bucket", bucket);
            partition.put("partition", record.getInt16("partition").toString());

            return new SourceRecord(partition, offset, topic,
                    Schemas.KEY_SCHEMA, key,
                    Schemas.VALUE_DEFAULT_SCHEMA, record);
        }
        return null;
    }

}
