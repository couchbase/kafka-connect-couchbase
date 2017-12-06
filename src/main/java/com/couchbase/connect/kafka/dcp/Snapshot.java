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

import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.Iterator;
import java.util.LinkedList;

public class Snapshot implements Event {
    private final short partition;
    private final long startSeqno;
    private final long endSeqno;
    private final LinkedList<ByteBuf> messages;

    public Snapshot(short partition, long startSeqno, long endSeqno) {
        this.partition = partition;
        this.startSeqno = startSeqno;
        this.endSeqno = endSeqno;
        this.messages = new LinkedList<ByteBuf>();
    }

    public short partition() {
        return partition;
    }

    public long startSeqno() {
        return startSeqno;
    }

    public long endSeqno() {
        return endSeqno;
    }

    public boolean add(ByteBuf message) {
        messages.add(message);
        return completed(message);
    }

    public boolean completed() {
        return completed(messages.getLast());
    }

    private boolean completed(ByteBuf message) {
        return DcpMutationMessage.bySeqno(message) == endSeqno;
    }

    @Override
    public String toString() {
        return "{" +
                "partition=" + partition +
                ", startSeqno=" + startSeqno +
                ", endSeqno=" + endSeqno +
                ", received=" + messages.size() +
                ", completed=" + completed() +
                '}';
    }

    @Override
    public Iterator<ByteBuf> iterator() {
        return messages.iterator();
    }

    @Override
    public void ack() {
    }
}
