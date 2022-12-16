/*
 * Copyright 2019 Couchbase, Inc.
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

import com.couchbase.client.dcp.highlevel.SnapshotMarker;
import com.couchbase.client.dcp.highlevel.StreamOffset;

import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.util.Objects.requireNonNull;

/**
 * Info required for resuming a DCP stream.
 */
class SourceOffset {
  private final StreamOffset streamOffset;

  public SourceOffset(StreamOffset offset) {
    this.streamOffset = requireNonNull(offset);
  }

  public SourceOffset withVbucketUuid(long vbucketUuid) {
    return new SourceOffset(
        new StreamOffset(
            vbucketUuid,
            streamOffset.getSeqno(),
            streamOffset.getSnapshot(),
            streamOffset.getCollectionsManifestUid()
        )
    );
  }

  public StreamOffset asStreamOffset() {
    return streamOffset;
  }

  public Map<String, Object> toMap() {
    return mapOf(
        "bySeqno", streamOffset.getSeqno(),
        "vbuuid", streamOffset.getVbuuid(),
        "snapshotStartSeqno", streamOffset.getSnapshot().getStartSeqno(),
        "snapshotEndSeqno", streamOffset.getSnapshot().getEndSeqno(),
        "collectionsManifestUid", streamOffset.getCollectionsManifestUid()
    );
  }

  public static SourceOffset fromMap(Map<String, Object> map) {
    // Only the sequence number is guaranteed to be present.
    // All other properties were added in later connector versions.
    long seqno = (Long) map.get("bySeqno");

    return new SourceOffset(
        new StreamOffset(
            (Long) map.getOrDefault("vbuuid", 0L),
            seqno,
            new SnapshotMarker(
                (Long) map.getOrDefault("snapshotStartSeqno", seqno),
                (Long) map.getOrDefault("snapshotEndSeqno", seqno)
            ),
            (Long) map.getOrDefault("collectionsManifestUid", 0L)
        ));
  }

  @Override
  public String toString() {
    return streamOffset.toString();
  }
}
