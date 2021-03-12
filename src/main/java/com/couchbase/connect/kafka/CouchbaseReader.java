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

import com.couchbase.client.dcp.Authenticator;
import com.couchbase.client.dcp.CertificateAuthenticator;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.PasswordAuthenticator;
import com.couchbase.client.dcp.SecurityConfig;
import com.couchbase.client.dcp.StaticCredentialsProvider;
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
import com.couchbase.connect.kafka.util.ConnectHelper;
import com.couchbase.connect.kafka.util.Version;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static com.couchbase.connect.kafka.util.JmxHelper.newJmxMeterRegistry;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class CouchbaseReader extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseReader.class);

  private final Client client;
  private final List<Integer> partitions;
  private final Map<Integer, SeqnoAndVbucketUuid> partitionToSavedSeqno;
  private final StreamFrom streamFrom;
  private final BlockingQueue<Throwable> errorQueue;

  public CouchbaseReader(CouchbaseSourceTaskConfig config, final String connectorName,
                         final BlockingQueue<DocumentChange> queue, final BlockingQueue<Throwable> errorQueue,
                         final List<Integer> partitions, final Map<Integer, SeqnoAndVbucketUuid> partitionToSavedSeqno,
                         final SourceDocumentLifecycle lifecycle) {
    requireNonNull(lifecycle);
    this.partitions = partitions;
    this.partitionToSavedSeqno = partitionToSavedSeqno;
    this.streamFrom = config.streamFrom();
    this.errorQueue = errorQueue;

    Authenticator authenticator = isNullOrEmpty(config.clientCertificatePath())
        ? new PasswordAuthenticator(new StaticCredentialsProvider(config.username(), config.password().value()))
        : CertificateAuthenticator.fromKeyStore(Paths.get(config.clientCertificatePath()), config.clientCertificatePassword().value());

    SecurityConfig.Builder securityConfig = SecurityConfig.builder()
        .enableTls(config.enableTls())
        .enableHostnameVerification(config.enableHostnameVerification());
    if (!isNullOrEmpty(config.trustStorePath())) {
      securityConfig.trustStore(Paths.get(config.trustStorePath()), config.trustStorePassword().value());
    }
    if (!isNullOrEmpty(config.trustCertificatePath())) {
      securityConfig.trustCertificate(Paths.get(config.trustCertificatePath()));
    }

    client = Client.builder()
        .userAgent("kafka-connector", Version.getVersion(), connectorName)
        .connectTimeout(config.bootstrapTimeout().toMillis())
        .seedNodes(config.dcpSeedNodes())
        .networkResolution(NetworkResolution.valueOf(config.network()))
        .bucket(config.bucket())
        .authenticator(authenticator)
        .collectionsAware(true)
        .scopeName(config.scope())
        .collectionNames(config.collections())
        .noValue(config.noValue())
        .xattrs(config.xattrs())
        .compression(config.compression())
        .mitigateRollbacks(config.persistencePollingInterval().toMillis(), TimeUnit.MILLISECONDS)
        .flowControl(config.flowControlBuffer().getByteCountAsSaturatedInt())
        .bufferAckWatermark(60)
        .securityConfig(securityConfig)
        .meterRegistry(newMeterRegistry(connectorName, config))
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
          lifecycle.logReceivedFromCouchbase(change);
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

  private static MeterRegistry newMeterRegistry(String connectorName, CouchbaseSourceTaskConfig config) {
    String taskId = ConnectHelper.getTaskIdFromLoggingContext().orElse(config.maybeTaskId());
    LinkedHashMap<String, String> commonKeyProperties = new LinkedHashMap<>();
    commonKeyProperties.put("connector", ObjectName.quote(connectorName));
    commonKeyProperties.put("task", taskId);
    return newJmxMeterRegistry("kafka.connect.couchbase", commonKeyProperties);
  }

  @Override
  public void run() {
    try {
      client.connect().block();

      // Apply the fallback state to all partitions. As of DCP client version 0.12.0,
      // this is the only way to set the sequence number to "now".
      StreamFrom fallbackStreamFrom = streamFrom.withoutSavedOffset();
      client.initializeState(fallbackStreamFrom.asDcpStreamFrom(), StreamTo.INFINITY).block();

      // Overlay any saved offsets (might have saved offsets for only some partitions).
      if (streamFrom.isSavedOffset()) {
        // As of DCP client version 0.12.0, Client.initializeState(BEGINNING, INFINITY)
        // doesn't fetch the failover logs. Do it ourselves to avoid a spurious rollback :-/
        initFailoverLogs();

        // Then restore the partition state and use saved vBucket UUIDs to overlay synthetic failover log entries.
        restoreSavedOffsets();
      }

      client.startStreaming(partitions).block();

    } catch (Throwable t) {
      errorQueue.offer(t);
    }
  }

  private void restoreSavedOffsets() {
    LOGGER.info("Resuming from saved offsets for {} of {} partitions",
        partitionToSavedSeqno.size(), partitions.size());

    for (Map.Entry<Integer, SeqnoAndVbucketUuid> entry : partitionToSavedSeqno.entrySet()) {
      final int partition = entry.getKey();
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
    client.failoverLogs(partitions).doOnNext(event -> {
      int partition = DcpFailoverLogResponse.vbucket(event);
      PartitionState ps = client.sessionState().get(partition);
      ps.setFailoverLog(DcpFailoverLogResponse.entries(event));
      client.sessionState().set(partition, ps);
    }).blockLast();
  }

  public void shutdown() {
    client.disconnect().block();
  }
}
