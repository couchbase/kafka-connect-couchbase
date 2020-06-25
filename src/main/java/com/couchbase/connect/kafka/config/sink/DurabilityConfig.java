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

package com.couchbase.connect.kafka.config.sink;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.connect.kafka.util.config.annotation.Default;

public interface DurabilityConfig {
  /**
   * The preferred way to specify an enhanced durability requirement
   * when using Couchbase Server 6.5 or later.
   * <p>
   * The default value of `NONE` means a write is considered
   * successful as soon as it reaches the memory of the active node.
   * <p>
   * NOTE: If you set this to anything other than `NONE`, then you
   * must not set `couchbase.persist.to` or `couchbase.replicate.to`.
   */
  @Default("NONE")
  DurabilityLevel durability();

  /**
   * For Couchbase Server versions prior to 6.5, this is how you require
   * the connector to verify a write is persisted to disk on a certain
   * number of replicas before considering the write successful.
   * <p>
   * If you're using Couchbase Server 6.5 or later, we recommend
   * using the `couchbase.durability` property instead.
   */
  @Default("NONE")
  PersistTo persistTo();

  /**
   * For Couchbase Server versions prior to 6.5, this is how you require
   * the connector to verify a write has reached the memory of a certain
   * number of replicas before considering the write successful.
   * <p>
   * If you're using Couchbase Server 6.5 or later, we recommend
   * using the `couchbase.durability` property instead.
   */
  @Default("NONE")
  ReplicateTo replicateTo();
}
