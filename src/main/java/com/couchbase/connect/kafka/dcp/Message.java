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

import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.Iterator;

public class Message implements Event {
    private final ByteBuf message;
    private final ChannelFlowController flowController;

    public Message(ByteBuf message, ChannelFlowController flowController) {
        this.message = message;
        this.flowController = flowController;
    }

    public ByteBuf message() {
        return message;
    }

    @Override
    public Iterator<ByteBuf> iterator() {
        return new SingleMessageIterator(message);
    }

    @Override
    public void ack() {
        flowController.ack(message);
    }

    private class SingleMessageIterator implements Iterator<ByteBuf> {
        private ByteBuf message;

        SingleMessageIterator(ByteBuf message) {
            this.message = message;
        }

        @Override
        public boolean hasNext() {
            return message != null;
        }

        @Override
        public ByteBuf next() {
            ByteBuf message = this.message;
            this.message = null;
            return message;
        }

        @Override
        public void remove() {
        }
    }
}
