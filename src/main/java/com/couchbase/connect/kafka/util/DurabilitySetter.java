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

package com.couchbase.connect.kafka.util;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.CommonDurabilityOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.connect.kafka.config.sink.DurabilityConfig;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.function.Consumer;

public interface DurabilitySetter extends Consumer<CommonDurabilityOptions<?>> {
  static DurabilitySetter create(DurabilityConfig config) {
    DurabilityLevel durabilityLevel = config.durability();
    if (durabilityLevel != DurabilityLevel.NONE) {
      if (config.persistTo() != PersistTo.NONE || config.replicateTo() != ReplicateTo.NONE) {
        throw new ConnectException("Invalid durability config. When 'couchbase.durability' is set, you must not set 'couchbase.replicate.to' or 'couchbase.persist.to'.");
      }

      return options -> options.durability(durabilityLevel);
    }

    PersistTo persistTo = config.persistTo();
    ReplicateTo replicateTo = config.replicateTo();
    return options -> options.durability(persistTo, replicateTo);
  }
}
