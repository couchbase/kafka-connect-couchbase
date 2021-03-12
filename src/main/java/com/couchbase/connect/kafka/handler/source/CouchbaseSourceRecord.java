/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.connect.kafka.handler.source;

import com.couchbase.client.dcp.highlevel.DocumentChange;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

/**
 * A source record with some metadata tacked on for document lifecycle tracing.
 */
public class CouchbaseSourceRecord extends SourceRecord {
  private final String qualifiedKey;
  private final long tracingToken;

  public CouchbaseSourceRecord(DocumentChange change,
                               Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
                               String topic, Integer partition,
                               Schema keySchema, Object key,
                               Schema valueSchema, Object value,
                               Long timestamp, Iterable<Header> headers) {
    super(sourcePartition, sourceOffset, topic, partition, keySchema, key, valueSchema, value, timestamp, headers);
    this.qualifiedKey = change.getQualifiedKey();
    this.tracingToken = change.getTracingToken();
  }

  public String getCouchbaseDocumentId() {
    return qualifiedKey;
  }

  public long getTracingToken() {
    return tracingToken;
  }
}
