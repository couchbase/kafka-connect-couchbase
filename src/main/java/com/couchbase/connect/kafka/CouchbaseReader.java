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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.dcp.Authenticator;
import com.couchbase.client.dcp.CertificateAuthenticator;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.PasswordAuthenticator;
import com.couchbase.client.dcp.SecurityConfig;
import com.couchbase.client.dcp.StaticCredentialsProvider;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import com.couchbase.client.dcp.highlevel.Deletion;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.highlevel.Mutation;
import com.couchbase.client.dcp.highlevel.StreamFailure;
import com.couchbase.client.dcp.highlevel.StreamOffset;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.highlevel.internal.FlowControlReceipt;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.StreamFlag;
import com.couchbase.client.dcp.metrics.LogLevel;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.dcp.util.PartitionSet;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceTaskConfig;
import com.couchbase.connect.kafka.util.FirstCallTracker;
import com.couchbase.connect.kafka.util.Version;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class CouchbaseReader extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseReader.class);

  static final String INITIAL_OFFSET_TOMBSTONE_KEY = "__COUCHBASE_INITIAL_OFFSET_TOMBSTONE__a54ee32b-4a7e-4d98-aa36-45d8417e942a";

  private final Client client;
  private final List<Integer> partitions;
  private final Map<Integer, SourceOffset> partitionToSavedOffset;
  private final StreamFrom streamFrom;
  private final boolean emitSyntheticInitialOffsets;
  private final BlockingQueue<DocumentChange> queue;
  private final BlockingQueue<Throwable> errorQueue;
  private final SourceTaskLifecycle taskLifecycle;
  private final CountDownLatch connectionAttemptComplete = new CountDownLatch(1);
  private final Duration bootstrapTimeout;
  private final FirstCallTracker shutdown = new FirstCallTracker();

  /**
   * As a workaround for JDCP-237 and the fact that the DCP client's `connect()` method
   * calls `disconnect()` internally if the connection failed, we need to keep track of
   * whether the connection was successful, so we don't end up calling `disconnect()`
   * if the client has already been disconnected due to a failed connection attempt.
   */
  private volatile boolean connected;

  static boolean isSyntheticInitialOffsetTombstone(DocumentChange e) {
    return INITIAL_OFFSET_TOMBSTONE_KEY.equals(e.getKey());
  }

  public CouchbaseReader(
      final CouchbaseSourceTaskConfig config,
      final String connectorName,
      final String taskNumber,
      final BlockingQueue<DocumentChange> queue,
      final BlockingQueue<Throwable> errorQueue,
      final List<Integer> partitions,
      final Map<Integer, SourceOffset> partitionToSavedOffset,
      final SourceDocumentLifecycle lifecycle,
      final MeterRegistry meterRegistry,
      final boolean emitSyntheticInitialOffsets,
      final SourceTaskLifecycle taskLifecycle
  ) {
    requireNonNull(connectorName);
    requireNonNull(lifecycle);
    this.queue = requireNonNull(queue);
    this.partitions = requireNonNull(partitions);
    this.partitionToSavedOffset = requireNonNull(partitionToSavedOffset);
    this.streamFrom = config.streamFrom();
    this.errorQueue = requireNonNull(errorQueue);
    this.emitSyntheticInitialOffsets = emitSyntheticInitialOffsets;
    this.taskLifecycle = requireNonNull(taskLifecycle);
    this.bootstrapTimeout = config.bootstrapTimeout();

    Authenticator authenticator = isNullOrEmpty(config.clientCertificatePath())
        ? new PasswordAuthenticator(new StaticCredentialsProvider(config.username(), config.password().value()))
        : CertificateAuthenticator.fromKeyStore(Paths.get(config.clientCertificatePath()), config.clientCertificatePassword().value());

    Consumer<SecurityConfig.Builder> securityConfig = security -> {
      security
          .enableTls(config.enableTls())
          .enableHostnameVerification(config.enableHostnameVerification());

      if (!isNullOrEmpty(config.trustStorePath())) {
        security.trustStore(Paths.get(config.trustStorePath()), config.trustStorePassword().value());
      }
      if (!isNullOrEmpty(config.trustCertificatePath())) {
        security.trustCertificate(Paths.get(config.trustCertificatePath()));
      }
      if (isNullOrEmpty(config.trustStorePath()) && isNullOrEmpty(config.trustCertificatePath())) {
        security.trustCertificates(com.couchbase.client.core.env.SecurityConfig.defaultCaCertificates());
      }
    };

    Client.Builder builder = Client.builder()
        .userAgent("kafka-connector", Version.getVersion(), connectorName, "task-" + taskNumber, "taskUuid=" + taskLifecycle.taskUuid())
        .bootstrapTimeout(config.bootstrapTimeout())
        .socketConnectTimeout(config.bootstrapTimeout().toMillis())
        .seedNodes(config.seedNodes())
        .networkResolution(NetworkResolution.valueOf(config.network()))
        .bucket(config.bucket())
        .authenticator(authenticator)
        .collectionsAware(true)
        .scopeName(config.scope())
        .collectionNames(config.collections())
        .optionalControlParam("change_streams", true)
        .optionalStreamFlags(setOf(StreamFlag.IGNORE_PURGED_TOMBSTONES))
        .noValue(config.noValue())
        .xattrs(config.xattrs())
        .compression(config.compression())
        .mitigateRollbacks(config.persistencePollingInterval().toMillis(), TimeUnit.MILLISECONDS)
        .flowControl(config.flowControlBuffer().getByteCountAsSaturatedInt())
        .bufferAckWatermark(60)
        .securityConfig(securityConfig)
        .meterRegistry(meterRegistry);

    if (config.enableDcpTrace()) {
      Pattern p = Pattern.compile(config.dcpTraceDocumentIdRegex());
      builder.trace(LogLevel.INFO,
          p.pattern().equals(".*")
              ? id -> true
              : id -> p.matcher(id).matches());
    }

    client = builder.build();

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

  @Override
  public void run() {
    try {
      client.connect().block();
      connected = true;

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

    } finally {
      connectionAttemptComplete.countDown();
    }
  }

  private static final FlowControlReceipt DUMMY_FLOW_CONTROL_RECEIPT = new FlowControlReceipt(ChannelFlowController.dummy, 0) {
    @Override
    public void acknowledge() {
    }
  };

  private void enqueueInitialOffsetTombstones() {
    if (!emitSyntheticInitialOffsets || streamFrom != StreamFrom.SAVED_OFFSET_OR_NOW) {
      return;
    }

    Set<Integer> partitionsWithoutSavedOffset = new HashSet<>(partitions);
    partitionsWithoutSavedOffset.removeAll(partitionToSavedOffset.keySet());

    partitionsWithoutSavedOffset.forEach(partition -> {
      // The Deletion constructor requires a DCP packet,
      // so create a synthetic deletion packet and populate
      // just enough of the fields to satisfy it.
      ByteBuf buf = Unpooled.buffer();
      MessageUtil.initRequest(MessageUtil.DCP_DELETION_OPCODE, buf);
      MessageUtil.setVbucket(partition, buf);

      Deletion syntheticInitialOffsetTombstone = new Deletion(
          buf,
          CollectionsManifest.DEFAULT.getCollection(0),
          INITIAL_OFFSET_TOMBSTONE_KEY,
          DUMMY_FLOW_CONTROL_RECEIPT,
          client.sessionState().get(partition).getOffset(),
          false
      );

      try {
        queue.add(syntheticInitialOffsetTombstone);
      } catch (Exception t) {
        throw new RuntimeException("Failed to enqueue synthetic initial offset", t);
      }
    });

    taskLifecycle.logMissingSourceOffsetsSetToNow(PartitionSet.from(partitionsWithoutSavedOffset));
  }

  private void restoreSavedOffsets() {
    LOGGER.info("Resuming from saved offsets for {} of {} partitions: {}",
        partitionToSavedOffset.size(),
        partitions.size(),
        PartitionSet.from(partitionToSavedOffset.keySet())
    );

    enqueueInitialOffsetTombstones();

    SortedMap<Integer, Long> partitionToFallbackUuid = new TreeMap<>();

    for (Map.Entry<Integer, SourceOffset> entry : partitionToSavedOffset.entrySet()) {
      final int partition = entry.getKey();
      final SourceOffset offset = entry.getValue();

      StreamOffset streamOffset = offset.asStreamOffset();

      if (streamOffset.getVbuuid() == 0) {
        // If we get here, we're probably restoring the stream offset from a previous version of the connector
        // which didn't save vbucket UUIDs. Hope the current vbuuid is the correct one.
        // CAVEAT: This doesn't always work, and sometimes triggers a rollback to zero.
        long currentVbuuid = client.sessionState().get(partition).getLastUuid();
        streamOffset = offset.withVbucketUuid(currentVbuuid).asStreamOffset();
        partitionToFallbackUuid.put(partition, currentVbuuid);
      }

      client.sessionState().set(partition, PartitionState.fromOffset(streamOffset));
    }

    if (!partitionToFallbackUuid.isEmpty()) {
      LOGGER.info(
          "Some source offsets are missing a partition UUID." +
              " This is normal if you're upgrading from connector version 3.4.5 or earlier." +
              " This message should go away after a document from each partition is published to Kafka." +
              " Here is the map from partition number to the latest partition UUID used as a fallback: {}",
          partitionToFallbackUuid
      );
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

  /**
   * There's no good way to interrupt an in-progress DCP client connection request,
   * so settle for waiting for it to succeed or fail.
   *
   * @return true if the attempt completed, or false if the attempt did not complete within the timeout.
   */
  private boolean waitForConnectionSuccessOrFailure() {
    try {
      Duration safeguardTimeout = bootstrapTimeout.multipliedBy(2).plus(Duration.ofSeconds(30));
      boolean attemptCompletedBeforeTimeout = connectionAttemptComplete.await(safeguardTimeout.toMillis(), TimeUnit.MILLISECONDS);
      if (!attemptCompletedBeforeTimeout) {
        LOGGER.error("Connection attempt did not succeed or fail within safeguard timeout of {}; taskUuid={}", safeguardTimeout, taskLifecycle.taskUuid());
      }
      return attemptCompletedBeforeTimeout;

    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while waiting for connection attempt completion; taskUuid={}", taskLifecycle.taskUuid());
      return false;
    }
  }

  public void shutdown() {
    if (shutdown.alreadyCalled()) {
      LOGGER.info("Ignoring redundant shutdown request; taskUuid={}", taskLifecycle.taskUuid());
      return;
    }

    if (connectionAttemptComplete.getCount() != 0) {
      LOGGER.info("CouchbaseReader was asked to shut down before it had finished connecting; deferring until connection succeeds or fails. taskUuid={}", taskLifecycle.taskUuid());
    }

    boolean completedWithinTimeout = waitForConnectionSuccessOrFailure();

    if (!connected && completedWithinTimeout) {
      LOGGER.info("Skipping DCP client disconnect because the connection attempt was unsuccessful. taskUuid={}", taskLifecycle.taskUuid());
      return;
    }

    if (completedWithinTimeout) {
      LOGGER.info("Disconnecting DCP client. taskUuid={}", taskLifecycle.taskUuid());
    } else {
      LOGGER.error("DCP client connection attempt did not succeed or fail within safeguard timeout. Making a last ditch effort to disconnect it. taskUuid={}", taskLifecycle.taskUuid());
    }

    try {
      client.disconnect().block();
    } catch (Exception e) {
      LOGGER.error("Failed to disconnect DCP client. taskUuid={}", taskLifecycle.taskUuid(), e);
      throw e;
    }
  }
}
