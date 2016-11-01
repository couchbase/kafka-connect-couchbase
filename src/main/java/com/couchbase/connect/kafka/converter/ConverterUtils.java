package com.couchbase.connect.kafka.converter;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

public class ConverterUtils {

    public static String bufToString(ByteBuf buf) {
        return new String(bufToBytes(buf), CharsetUtil.UTF_8);
    }

    public static byte[] bufToBytes(ByteBuf buf) {
        byte[] bytes;
        bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
    }
}
