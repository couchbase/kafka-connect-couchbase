/*
 * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.connect.kafka.config.sink;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.connect.kafka.util.config.DataSize;
import com.couchbase.connect.kafka.util.config.annotation.Default;

public interface AnalyticsSinkHandlerConfig {
  /**
   * Every Batch consists of an UPSERT or a DELETE statement,
   * based on mutations.
   * This property determines the maximum number of records
   * in the UPSERT or DELETE statement in the batch. Users can configure
   * this parameter based on the capacity of their analytics cluster.
   * <p>
   * This property is specific to `AnalyticsSinkHandler`.
   *
   * @since 4.1.14
   */
  @Stability.Uncommitted
  @Default("100")
  int analyticsMaxRecordsInBatch();

  /**
   * Every Batch consists of an UPSERT or a DELETE statement,
   * based on mutations.
   * This property defines the max size of all docs in bytes in an UPSERT statement in a batch.
   * Users can configure this parameter based on the capacity of their analytics cluster.
   * <p>
   * This property is specific to `AnalyticsSinkHandler`.
   *
   * @since 4.1.15
   */
  @Stability.Uncommitted
  @Default("5m")
  DataSize analyticsMaxSizeInBatch();
}
