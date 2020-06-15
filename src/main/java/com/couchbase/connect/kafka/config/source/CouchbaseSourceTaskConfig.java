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

import java.util.List;

public interface CouchbaseSourceTaskConfig extends CouchbaseSourceConfig {
  /**
   * List of partitions for this task to watch for changes.
   */
  List<String> partitions(); // each item is a stringified integer.

  /**
   * KV node addresses the DCP client should bootstrap against.
   * <p>
   * These addresses come from the Java client, so DNS SRV
   * and other complexities have already been handled.
   */
  List<String> dcpSeedNodes();
}
