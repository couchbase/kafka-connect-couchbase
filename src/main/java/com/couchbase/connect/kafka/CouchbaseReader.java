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
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.connect.kafka.dcp.Event;
import com.couchbase.connect.kafka.dcp.Message;
import com.couchbase.connect.kafka.dcp.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action1;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class CouchbaseReader extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseReader.class);

    private final Client client;
    private final Short[] partitions;
    private final SessionState initialSessionState;
    private final Map<Short, Snapshot> snapshots;

    public CouchbaseReader(List<String> clusterAddress, String bucket, String password, long connectionTimeout,
                           final BlockingQueue<Event> queue, Short[] partitions, SessionState sessionState,
                           final boolean useSnapshots, final boolean sslEnabled, final String sslKeystoreLocation,
                           final String sslKeystorePassword) {
        this.snapshots = new ConcurrentHashMap<Short, Snapshot>(partitions.length);
        this.partitions = partitions;
        this.initialSessionState = sessionState;
        client = Client.configure()
                .connectTimeout(connectionTimeout)
                .hostnames(clusterAddress)
                .bucket(bucket)
                .password(password)
                .controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, 20480)
                .bufferAckWatermark(60)
                .sslEnabled(sslEnabled)
                .sslKeystoreFile(sslKeystoreLocation)
                .sslKeystorePassword(sslKeystorePassword)
                .build();
        client.controlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                if (useSnapshots) {
                    if (DcpSnapshotMarkerRequest.is(event)) {
                        Snapshot snapshot = new Snapshot(
                                DcpSnapshotMarkerRequest.partition(event),
                                DcpSnapshotMarkerRequest.startSeqno(event),
                                DcpSnapshotMarkerRequest.endSeqno(event)
                        );
                        Snapshot prev = snapshots.put(snapshot.partition(), snapshot);
                        if (prev != null) {
                            LOGGER.warn("Incomplete snapshot detected: {}", prev);
                        }
                    }
                }
                client.acknowledgeBuffer(event);
                event.release();
            }
        });
        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                if (useSnapshots) {
                    short partition = DcpMutationMessage.partition(event);
                    long seqno = DcpMutationMessage.bySeqno(event);

                    Snapshot snapshot = snapshots.get(partition);
                    if (snapshot == null) {
                        LOGGER.warn("Event with seqno {} for partition {} ignored, because missing snapshot", seqno, partition);
                    } else if (seqno < snapshot.startSeqno()) {
                        LOGGER.warn("Event with seqno {} for partition {} ignored, because current snapshot has higher seqno {}",
                                seqno, partition, snapshot.startSeqno());
                    } else {
                        event.retain();
                        boolean completed = snapshot.add(event);
                        if (completed) {
                            Snapshot oldSnapshot = snapshots.remove(partition);
                            if (snapshot != oldSnapshot) {
                                LOGGER.warn("Conflict of snapshots detected, expected to remove {}, but removed {}", snapshot, oldSnapshot);
                            }
                            queue.add(snapshot);
                        }
                    }
                    client.acknowledgeBuffer(event);
                    event.release();
                } else {
                    try {
                        queue.put(new Message(event));
                    } catch (InterruptedException e) {
                        LOGGER.error("Unable to put DCP request into the queue", e);
                    }
                }
            }
        });
    }

    public void acknowledge(Event event) {
        if (event instanceof Message) {
            client.acknowledgeBuffer(((Message) event).message());
        }
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
