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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.CouchbaseBucketTopology;
import com.couchbase.client.java.Bucket;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class CouchbaseHelper {
  public static Mono<ClusterTopologyWithBucket> getConfig(Core core, String bucketName) {
    return core
        .configurationProvider()
        .configs()
        .flatMap(clusterConfig ->
            Mono.justOrEmpty(clusterConfig.bucketTopology(bucketName)))
        .filter(CouchbaseHelper::hasPartitionInfo)
        .next();
  }

  /**
   * Returns true unless the topology is from a newly-created bucket
   * whose partition count is not yet available.
   */
  private static boolean hasPartitionInfo(ClusterTopologyWithBucket topology) {
    CouchbaseBucketTopology bucketTopology = (CouchbaseBucketTopology) topology.bucket();
    return bucketTopology.numberOfPartitions() > 0;
  }


  public static Mono<ClusterTopologyWithBucket> getConfig(Bucket bucket) {
    return getConfig(bucket.core(), bucket.name());
  }

  public static ClusterTopologyWithBucket getConfig(Bucket bucket, Duration timeout) {
    return getConfig(bucket).block(timeout);
  }
}
