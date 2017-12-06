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

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.connect.kafka.handler.source.SourceHandler;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * General interface to convert a couchbase DCP event to Kafka SourceRecord.
 *
 * @author mstadelmann@atex.com
 * @deprecated In favor of {@link SourceHandler}. Scheduled for removal in 4.0.0.
 */
@Deprecated
public interface Converter {

    /**
     * Convert a DCP message to a SourceRecord.
     *
     * @param byteBuf The DCP message as a ByteBuf.
     * @param bucket The bucket.
     * @param topic The topic.
     * @return A converted SourceRecord.
     */
    SourceRecord convert(final ByteBuf byteBuf, final String bucket, final String topic);
}
