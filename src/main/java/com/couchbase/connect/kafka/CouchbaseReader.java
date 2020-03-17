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

import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.highlevel.SnapshotMarker;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.IllegalReferenceCountException;
import com.couchbase.connect.kafka.dcp.Event;
import com.couchbase.connect.kafka.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.CompletableSubscriber;
import rx.Subscription;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

public class CouchbaseReader extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseReader.class);

  private final Client client;
  private final Short[] partitions;
  private final Map<Short, SeqnoAndVbucketUuid> partitionToSavedSeqno;
  private final StreamFrom streamFrom;
  private final BlockingQueue<Throwable> errorQueue;

  public CouchbaseReader(final String connectorName, List<String> clusterAddress, String bucket, String username, String password, long connectionTimeout,
                         final BlockingQueue<Event> queue, final BlockingQueue<Throwable> errorQueue, Short[] partitions,
                         final Map<Short, SeqnoAndVbucketUuid> partitionToSavedSeqno, final StreamFrom streamFrom,
                         final boolean sslEnabled, final String sslKeystoreLocation,
                         final String sslKeystorePassword, final CompressionMode compressionMode,
                         long persistencePollingIntervalMillis, int flowControlBufferBytes, NetworkResolution networkResolution) {
    this.partitions = partitions;
    this.partitionToSavedSeqno = partitionToSavedSeqno;
    this.streamFrom = streamFrom;
    this.errorQueue = errorQueue;
    client = Client.builder()
        .userAgent("kafka-connector", Version.getVersion(), connectorName)
        .connectTimeout(connectionTimeout)
        .hostnames(clusterAddress)
        .networkResolution(networkResolution)
        .bucket(bucket)
        .credentials(username, password)
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
        try {
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
                    errorQueue.offer(e);
                  }

                  @Override
                  public void onSubscribe(Subscription d) {
                  }
                });
          }

        } catch (Throwable t) {
          LOGGER.error("Exception in control event handler", t);
          errorQueue.offer(t);
        } finally {
          ackAndRelease(flowController, event);
        }
      }
    });
    client.dataEventHandler(new DataEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        try {
          long vbucketUuid = getVBucketUuid(MessageUtil.getVbucket(event));
          queue.put(new Event(event, vbucketUuid, flowController));
        } catch (Throwable t) {
          LOGGER.error("Unable to put DCP request into the queue", t);
          ackAndRelease(flowController, event);
          errorQueue.offer(t);
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
        // As of DCP client version 0.12.0, Client.initializeState(BEGINNING, INFINITY)
        // doesn't fetch the failover logs. Do it ourselves to avoid a spurious rollback :-/
        initFailoverLogs();

        // Then restore the partition state and use saved vBucket UUIDs to overlay synthetic failover log entries.
        restoreSavedOffsets();
      }

      client.startStreaming(partitions).await();

    } catch (Throwable t) {
      errorQueue.offer(t);
    }
  }

  private void restoreSavedOffsets() {
    LOGGER.info("Resuming from saved offsets for {} of {} partitions",
        partitionToSavedSeqno.size(), partitions.length);

    for (Map.Entry<Short, SeqnoAndVbucketUuid> entry : partitionToSavedSeqno.entrySet()) {
      final short partition = entry.getKey();
      final SeqnoAndVbucketUuid offset = entry.getValue();
      final long savedSeqno = offset.seqno();

      PartitionState ps = client.sessionState().get(partition);
      ps.setStartSeqno(savedSeqno);
      ps.setSnapshot(new SnapshotMarker(savedSeqno, savedSeqno));

      if (offset.vbucketUuid().isPresent()) {
        long vbuuid = offset.vbucketUuid().getAsLong();
        LOGGER.debug("Initializing failover log for partition {} using stored vbuuid {} ", partition, vbuuid);
        // Use seqno -1 (max unsigned) so this synthetic failover log entry will always be pruned
        // if the initial streamOpen request gets a rollback response. If there's no rollback
        // on initial request, then the seqno used here doesn't matter, because the failover log
        // gets reset when the stream is opened.
        ps.setFailoverLog(singletonList(new FailoverLogEntry(-1L, vbuuid)));
      } else {
        // If we get here, we're probably restoring the stream offset from a previous version of the connector
        // which didn't save vbucket UUIDs. Hope the current vbuuid in the failover log is the correct one.
        // CAVEAT: This doesn't always work, and sometimes triggers a rollback to zero.
        LOGGER.warn("No vBucket UUID is associated with stream offset for partition {}." +
            " This is normal if you're upgrading from connector version 3.4.5 or earlier," +
            " and should stop happening once the Kafka Connect framework asks the connector" +
            " to save its offsets (see connector worker config property 'offset.flush.interval.ms').", partition);
      }
      client.sessionState().set(partition, ps);
    }
  }

  private void initFailoverLogs() {
    client.failoverLogs(partitions).toBlocking().forEach(event -> {
      short partition = DcpFailoverLogResponse.vbucket(event);
      PartitionState ps = client.sessionState().get(partition);
      ps.setFailoverLog(DcpFailoverLogResponse.entries(event));
      client.sessionState().set(partition, ps);
    });
  }

  private long getVBucketUuid(int vBucketId) {
    return client.sessionState().get(vBucketId).getLastUuid();
  }

  public void shutdown() {
    client.disconnect().await();
  }

  private static void ackAndRelease(ChannelFlowController flowController, ByteBuf buffer) throws IllegalReferenceCountException {
    ack(flowController, buffer);
    buffer.release();
  }

  private static void ack(ChannelFlowController flowController, ByteBuf buffer) throws IllegalReferenceCountException {
    try {
      flowController.ack(buffer);

    } catch (IllegalReferenceCountException e) {
      throw e;

    } catch (Exception e) {
      LOGGER.warn("Flow control ack failed (channel already closed?)", e);
    }
  }
}
