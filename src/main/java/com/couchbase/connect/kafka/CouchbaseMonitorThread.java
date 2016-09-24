/**
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

package com.couchbase.connect.kafka;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerMessage;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class CouchbaseMonitorThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseMonitorThread.class);

    private final long connectionTimeout;
    private final Client client;
    private final Short[] partitions;
    private final SessionState initialSessionState;

    public CouchbaseMonitorThread(List<String> clusterAddress, String bucket, String password, long connectionTimeout,
                                  final BlockingQueue<ByteBuf> queue, Short[] partitions, SessionState sessionState) {
        this.connectionTimeout = connectionTimeout;
        this.partitions = partitions;
        this.initialSessionState = sessionState;
        client = Client.configure()
                .hostnames(clusterAddress)
                .bucket(bucket)
                .password(password)
                .controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, 20480)
                .bufferAckWatermark(60)
                .build();
        client.controlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                if (DcpSnapshotMarkerMessage.is(event)) {
                    client.acknowledgeBuffer(event);
                }
                event.release();
            }
        });
        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                try {
                    queue.put(event);
                } catch (InterruptedException e) {
                    LOGGER.error("Unable to put DCP request into the queue", e);
                }
            }
        });
    }

    public void acknowledgeBuffer(ByteBuf event) {
        client.acknowledgeBuffer(event);
    }

    @Override
    public void run() {
        client.connect().await(); // FIXME: uncomment and raise timeout exception: .await(connectionTimeout, TimeUnit.MILLISECONDS);
        client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).await();
        client.failoverLogs(partitions).forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf event) {
                short partition = DcpFailoverLogResponse.vbucket(event);
                int numEntries = DcpFailoverLogResponse.numLogEntries(event);
                PartitionState ps = initialSessionState.get(partition);
                for (int i = 0; i < numEntries; i++) {
                    ps.addToFailoverLog(
                            DcpFailoverLogResponse.seqnoEntry(event, i),
                            DcpFailoverLogResponse.vbuuidEntry(event, i)
                    );
                }
                client.sessionState().set(partition, ps);
            }
        });
        client.startStreaming(partitions).await();
    }

    public void shutdown() {
        client.disconnect().await();
    }
}
