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

package com.couchbase.connect.kafka.util.config;

public class DataSize {
  private final long byteCount;

  private DataSize(long byteCount) {
    this.byteCount = byteCount;
  }

  public static DataSize ofBytes(long bytes) {
    return new DataSize(bytes);
  }

  public long getByteCount() {
    return byteCount;
  }

  public int getByteCountAsSaturatedInt() {
    return (int) Math.min(Integer.MAX_VALUE, byteCount);
  }

  @Override
  public String toString() {
    return byteCount + " bytes";
  }
}
