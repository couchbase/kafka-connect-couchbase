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

import java.util.OptionalLong;

/**
 * Info required for resuming a DCP stream.
 */
class SourceOffset {
  private final long seqno;
  private final OptionalLong vbucketUuid;

  public SourceOffset(long seqno, Long vbucketUuid) {
    this.seqno = seqno;
    this.vbucketUuid = vbucketUuid == null ? OptionalLong.empty() : OptionalLong.of(vbucketUuid);
  }

  @Override
  public String toString() {
    return vbucketUuid + "@" + seqno;
  }

  public long seqno() {
    return seqno;
  }

  public OptionalLong vbucketUuid() {
    return vbucketUuid;
  }
}
