/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.connect.kafka.config.source;

import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.util.config.DataSize;
import com.couchbase.connect.kafka.util.config.annotation.Default;
import com.couchbase.connect.kafka.util.config.annotation.Dependents;
import com.couchbase.connect.kafka.util.config.annotation.Group;

import java.time.Duration;

@Group("Database Change Protocol")
public interface DcpConfig {
  /**
   * To reduce bandwidth usage, Couchbase Server 5.5 and later can send documents
   * to the connector in compressed form. (Messages are always published to the
   * Kafka topic in uncompressed form, regardless of this setting.)
   */
  @Default("ENABLED")
  CompressionMode compression();

  /**
   * When a Couchbase Server node fails over, documents on the failing node
   * that haven't been fully replicated may be "rolled back" to a previous state.
   * To ensure consistency between Couchbase and the Kafka topic, the connector
   * can defer publishing a document to Kafka until it has been saved to disk
   * on all replicas.
   * <p>
   * To enable this feature, specify a non-zero persistence polling interval.
   * The interval is how frequently the connector asks each Couchbase node
   * which changes have been fully replicated and persisted. This ensures
   * consistency between Couchbase and Kafka, at the cost of additional latency
   * and bandwidth usage.
   * <p>
   * To disable this feature, specify a zero duration (`0`). In this mode the
   * connector publishes changes to Kafka immediately, without waiting for
   * replication. This is fast and uses less network bandwidth, but can result
   * in publishing "phantom changes" that don’t reflect the actual state of
   * a document in Couchbase after a failover.
   * <p>
   * TIP: Documents written to Couchbase with enhanced durability are never
   * published to Kafka until the durability requirements are met, regardless
   * of whether persistence polling is enabled.
   * <p>
   * CAUTION: When connecting to an ephemeral bucket, always disable
   * persistence polling by setting this config option to `0`,
   * otherwise the connector will never publish any changes.
   */
  @Default("100ms")
  Duration persistencePollingInterval();

  /**
   * The flow control buffer limits how much data Couchbase will send before
   * waiting for the connector to acknowledge the data has been processed.
   * The recommended size is between 10 MiB ("10m") and 50 MiB ("50m").
   * <p>
   * CAUTION: Make sure to allocate enough memory to the Kafka Connect worker
   * process to accommodate the flow control buffer, otherwise the connector
   * might run out of memory under heavy load. Read on for details.
   * <p>
   * There's a separate buffer for each node in the Couchbase cluster. When
   * calculating how much memory to allocate to the Kafka Connect worker,
   * multiply the flow control buffer size by the number of Couchbase nodes,
   * then multiply by 2. This is how much memory a single connector task
   * requires for the flow control buffer (not counting the connector's
   * baseline memory usage).
   */
  @Default("16m")
  DataSize flowControlBuffer();

  /**
   * Should filters and source handlers have access to a document's extended attributes?
   *
   * @see DocumentEvent#xattrs()
   */
  @Default("false")
  boolean xattrs();

  /**
   * If true, detailed protocol trace information is logged to the
   * `com.couchbase.client.dcp.trace` category at INFO level.
   * Otherwise, trace information is not logged.
   * <p>
   * Disabled by default because it generates many log messages.
   *
   * @since 4.1.6
   */
  @Default("false")
  @Dependents("couchbase.dcp.trace.document.id.regex")
  boolean enableDcpTrace();

  /**
   * When DCP trace is enabled, set this property to limit the trace
   * to only documents whose IDs match this Java regular expression.
   * <p>
   * Ignored if `couchbase.enable.dcp.trace` is false.
   *
   * @since 4.1.6
   */
  @Default(".*")
  String dcpTraceDocumentIdRegex();
}
