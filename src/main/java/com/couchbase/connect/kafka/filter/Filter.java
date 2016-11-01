package com.couchbase.connect.kafka.filter;


import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

/**
 * General interface to select Couchbase events, which has to be sent to Kafka.
 *
 * @author mstadelmann@atex.com
 */
public interface Filter {

    /**
     * Decides whether <code>message</code> should be sent to Kafka.
     *
     * @param message DCP event message from Couchbase.
     * @return true if event should be sent to Kafka.
     */
    boolean pass(ByteBuf message);

}
