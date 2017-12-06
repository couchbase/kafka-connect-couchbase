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
