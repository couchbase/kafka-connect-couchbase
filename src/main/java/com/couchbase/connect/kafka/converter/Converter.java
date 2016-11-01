package com.couchbase.connect.kafka.converter;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * General interface to convert a couchbase DCP event to Kafka SourceRecord.
 *
 * @author mstadelmann@atex.com
 */
public interface Converter {

    /**
     * Convert a DCP message to a SourceRecord.
     * @param byteBuf The DCP message as a ByteBuf.
     * @param bucket The bucket.
     * @param topic The topic.
     * @return A converted SourceRecord.
     */
    SourceRecord convert(final ByteBuf byteBuf, final String bucket, final String topic);
}
