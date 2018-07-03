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

package com.couchbase.connect.kafka;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.connect.kafka.dcp.Event;
import com.couchbase.connect.kafka.dcp.Message;
import com.couchbase.connect.kafka.dcp.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.CompletableSubscriber;
import rx.Subscription;
import rx.functions.Action1;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CouchbaseReader extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseReader.class);

    private final Client client;
    private final Short[] partitions;
    private final Map<Short, Long> partitionToSavedSeqno;
    private final StreamFrom streamFrom;
    private final Map<Short, Snapshot> snapshots;
    private final BlockingQueue<Throwable> errorQueue;

    public CouchbaseReader(List<String> clusterAddress, String bucket, String username, String password, long connectionTimeout,
                           final BlockingQueue<Event> queue, BlockingQueue<Throwable> errorQueue, Short[] partitions,
                           final Map<Short, Long> partitionToSavedSeqno, final StreamFrom streamFrom,
                           final boolean useSnapshots, final boolean sslEnabled, final String sslKeystoreLocation,
                           final String sslKeystorePassword, final CompressionMode compressionMode,
                           long persistencePollingIntervalMillis, int flowControlBufferBytes) {
        this.snapshots = new ConcurrentHashMap<Short, Snapshot>(partitions.length);
        this.partitions = partitions;
        this.partitionToSavedSeqno = partitionToSavedSeqno;
        this.streamFrom = streamFrom;
        this.errorQueue = errorQueue;
        client = Client.configure()
                .connectTimeout(connectionTimeout)
                .hostnames(clusterAddress)
                .bucket(bucket)
                .username(username)
                .password(password)
                .controlParam(DcpControl.Names.ENABLE_NOOP, "true")
                .compression(compressionMode)
                .mitigateRollbacks(persistencePollingIntervalMillis, TimeUnit.MILLISECONDS)
                .flowControl(flowControlBufferBytes)
                .bufferAckWatermark(60)
                .sslEnabled(sslEnabled)
                .sslKeystoreFile(sslKeystoreLocation)
                .sslKeystorePassword(sslKeystorePassword)
                .build();
        client.controlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(ChannelFlowController flowController, ByteBuf event) {
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

                if (RollbackMessage.is(event)) {
                    final short partition = RollbackMessage.vbucket(event);
                    final long seqno = RollbackMessage.seqno(event);

                    LOGGER.warn("Rolling back partition {} to seqno {}", partition, seqno);

                    // Careful, we're in the Netty IO thread, so must not await completion.
                    client.rollbackAndRestartStream(partition, seqno)
                            .subscribe(new CompletableSubscriber() {
                                @Override
                                public void onCompleted() {
                                    LOGGER.info("Rollback for partition {} complete", partition);
                                }

                                @Override
                                public void onError(Throwable e) {
                                    LOGGER.error("Failed to roll back partition {} to seqno {}", partition, seqno, e);
                                }

                                @Override
                                public void onSubscribe(Subscription d) {
                                }
                            });
                }

                flowController.ack(event);
                event.release();
            }
        });
        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ChannelFlowController flowController, ByteBuf event) {
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
                    flowController.ack(event);
                    event.release();
                } else {
                    try {
                        queue.put(new Message(event, flowController));
                    } catch (InterruptedException e) {
                        LOGGER.error("Unable to put DCP request into the queue", e);
                    }
                }
            }
        });
    }

    @Override
    public void run() {
        try {
            client.connect().await(); // FIXME: uncomment and raise timeout exception: .await(connectionTimeout, TimeUnit.MILLISECONDS);

            // Apply the fallback state to all partitions. As of DCP client version 0.12.0,
            // this is the only way to set the sequence number to "now".
            StreamFrom fallbackStreamFrom = streamFrom.withoutSavedOffset();
            client.initializeState(fallbackStreamFrom.asDcpStreamFrom(), StreamTo.INFINITY).await();

            // Overlay any saved offsets (might have saved offsets for only some partitions).
            if (streamFrom.isSavedOffset()) {
                restoreSavedOffsets();

                // As of DCP client version 0.12.0, Client.initializeState(BEGINNING, INFINITY)
                // doesn't fetch the failover logs. Do it ourselves to avoid a spurious rollback :-/
                initFailoverLogs();
            }

            client.startStreaming(partitions).await();

        } catch (Throwable t) {
            errorQueue.add(t);
        }
    }

    private void restoreSavedOffsets() {
        LOGGER.info("Resuming from saved offsets for {} of {} partitions",
                partitionToSavedSeqno.size(), partitions.length);

        for (Map.Entry<Short, Long> entry : partitionToSavedSeqno.entrySet()) {
            final short partition = entry.getKey();
            final long savedSeqno = entry.getValue();
            PartitionState ps = client.sessionState().get(partition);
            ps.setStartSeqno(savedSeqno);
            ps.setSnapshotStartSeqno(savedSeqno);
            ps.setSnapshotEndSeqno(savedSeqno);
            client.sessionState().set(partition, ps);
        }
    }

    private void initFailoverLogs() {
        client.failoverLogs(partitions).toBlocking().forEach(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf event) {
                short partition = DcpFailoverLogResponse.vbucket(event);
                int numEntries = DcpFailoverLogResponse.numLogEntries(event);
                PartitionState ps = client.sessionState().get(partition);
                ps.getFailoverLog().clear();
                for (int i = 0; i < numEntries; i++) {
                    ps.addToFailoverLog(
                            DcpFailoverLogResponse.seqnoEntry(event, i),
                            DcpFailoverLogResponse.vbuuidEntry(event, i)
                    );
                }
                client.sessionState().set(partition, ps);
            }
        });
    }

    long getVBucketUuid(int vBucketId) {
        return client.sessionState().get(vBucketId).getLastUuid();
    }

    public void shutdown() {
        client.disconnect().await();
    }
}
