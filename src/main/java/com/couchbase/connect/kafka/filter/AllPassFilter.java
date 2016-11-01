package com.couchbase.connect.kafka.filter;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class AllPassFilter implements Filter {

    @Override
    public boolean pass(final ByteBuf message) {
        return true;
    }
}
