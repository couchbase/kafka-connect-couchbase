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
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.core.env.NetworkResolution;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import com.couchbase.client.dcp.highlevel.Deletion;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.highlevel.Mutation;
import com.couchbase.client.dcp.highlevel.SnapshotMarker;
import com.couchbase.client.dcp.highlevel.StreamFailure;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceTaskConfig;
import com.couchbase.connect.kafka.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public CouchbaseReader(CouchbaseSourceTaskConfig config, final String connectorName,
                         final BlockingQueue<DocumentChange> queue, final BlockingQueue<Throwable> errorQueue,
                         final Short[] partitions, final Map<Short, SeqnoAndVbucketUuid> partitionToSavedSeqno) {
    this.partitions = partitions;
    this.partitionToSavedSeqno = partitionToSavedSeqno;
    this.streamFrom = config.streamFrom();
    this.errorQueue = errorQueue;
    client = Client.builder()
        .userAgent("kafka-connector", Version.getVersion(), connectorName)
        .connectTimeout(config.bootstrapTimeout().toMillis())
        .seedNodes(config.dcpSeedNodes())
        .networkResolution(NetworkResolution.valueOf(config.network()))
        .bucket(config.bucket())
        .credentials(config.username(), config.password().value())
        .collectionsAware(true)
        .scopeName(config.scope())
        .collectionNames(config.collections())
        .compression(config.compression())
        .mitigateRollbacks(config.persistencePollingInterval().toMillis(), TimeUnit.MILLISECONDS)
        .flowControl(config.flowControlBuffer().getByteCountAsSaturatedInt())
        .bufferAckWatermark(60)
        .sslEnabled(config.enableTls())
        .sslKeystoreFile(config.trustStorePath())
        .sslKeystorePassword(config.trustStorePassword().value())
        .build();

    client.nonBlockingListener(new DatabaseChangeListener() {
      @Override
      public void onMutation(Mutation mutation) {
        onChange(mutation);
      }

      @Override
      public void onDeletion(Deletion deletion) {
        onChange(deletion);
      }

      private void onChange(DocumentChange change) {
        try {
          queue.put(change);

        } catch (Throwable t) {
          change.flowControlAck();
          LOGGER.error("Unable to put DCP request into the queue", t);
          errorQueue.offer(t);
        }
      }

      @Override
      public void onFailure(StreamFailure streamFailure) {
        errorQueue.offer(streamFailure.getCause());
      }
    });
  }

  @Override
  public void run() {
    try {
      client.connect().await();

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

  public void shutdown() {
    client.disconnect().await();
  }
}
