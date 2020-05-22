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
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf;

import static java.util.Objects.requireNonNull;

public class Event {
  private final ByteBuf message;
  private final ChannelFlowController flowController;
  private final long vbucketUuid;

  public Event(ByteBuf message, long vbucketUuid, ChannelFlowController flowController) {
    this.message = requireNonNull(message);
    this.flowController = requireNonNull(flowController);
    this.vbucketUuid = vbucketUuid;
  }

  public ByteBuf message() {
    return message;
  }

  public long vbucketUuid() {
    return vbucketUuid;
  }

  public void ack() {
    flowController.ack(message);
  }
}
