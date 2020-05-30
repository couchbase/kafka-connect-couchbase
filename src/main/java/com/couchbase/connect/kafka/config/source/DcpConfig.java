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
import com.couchbase.connect.kafka.util.config.DataSize;
import com.couchbase.connect.kafka.util.config.annotation.Default;
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
   * How frequently to poll Couchbase Server to see which changes are ready
   * to be published. Specify `0` to disable polling, or an integer followed
   * by a time qualifier (example: 100ms)
   */
  @Default("100ms")
  Duration persistencePollingInterval();

  /**
   * How much heap space should be allocated to the flow control buffer.
   * Specify an integer followed by a size qualifier (example: 128m)
   */
  @Default("128m")
  DataSize flowControlBufferSize();
}
