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
package com.couchbase.connect.kafka.util;

import com.couchbase.connect.kafka.handler.sink.ConcurrencyHint;
import com.couchbase.connect.kafka.util.N1qlData.OperationType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AnalyticsBatchBuilder {
  private final List<Batch> batches = new ArrayList<>();
  private final int maxBatchSize;

  public AnalyticsBatchBuilder(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
  }

  public void add(N1qlData data) {
    ConcurrencyHint hint = data.getHint();
    String keyspace = data.getKeyspace();
    N1qlData.OperationType type = data.getType();

    Batch batch = getBatch(keyspace, hint, type);
    batch.add(data);
  }


  private Batch getBatch(String keyspace, ConcurrencyHint hint, N1qlData.OperationType type) {
    if (!batches.isEmpty()) {
      Batch currentBatch = batches.get(batches.size() - 1);
      if (currentBatch.isCompatible(keyspace, type, hint)) {
        return currentBatch;
      }
    }
    // If currentBatch is not suitable based on keyspace or
    // batch size reached threshold , then create a new Batch
    Batch batch = new Batch(keyspace, type, batches.size(), maxBatchSize);
    batches.add(batch);

    return batch;
  }

  public List<String> build() {
    return batches.stream().map(Batch::getBlockQuery).collect(Collectors.toList());
  }


  private static class Batch {
    // These three parameters uniquely identify a batch
    private final N1qlData.OperationType type;
    private final String keyspace;
    private final Set<ConcurrencyHint> hints = new HashSet<>();
    private final StringBuilder batchedData = new StringBuilder();
    private final int batchId;
    private final int batchLimit;
    private int countOfRecordsInCurrentBatch = 0;

    Batch(String keyspace, N1qlData.OperationType type, int batchId, int batchLimit) {
      this.type = type;
      this.keyspace = keyspace;
      this.batchId = batchId;
      this.batchLimit = batchLimit;
    }

    public N1qlData.OperationType getType() {
      return type;
    }

    void add(N1qlData data) {
      hints.add(data.getHint());

      switch (data.getType()) {
        case UPSERT:
          if (batchedData.length() != 0) {
            batchedData.append(" , ");
          }
          batchedData.append(data.getData());
          countOfRecordsInCurrentBatch++;
          break;
        case DELETE:
          if (batchedData.length() != 0) {
            batchedData.append(" OR ");
          }
          batchedData.append(data.getData());
          countOfRecordsInCurrentBatch++;
          break;
        default:
          throw new IllegalArgumentException("No Type " + data.getType() + " Found");
      }
    }

    public int getCountOfRecordsInCurrentBatch() {
      return countOfRecordsInCurrentBatch;
    }

    public boolean isCompatible(String keyspace, OperationType type, ConcurrencyHint hint) {
      return this.keyspace.equals(keyspace) && this.type.equals(type) && !hints.contains(hint) && getCountOfRecordsInCurrentBatch() < batchLimit;
    }

    public String getBlockQuery() {
      String queryFromBatchedData;
      switch (type) {
        case UPSERT:
          // UPSERT INTO <keyspace> ([ data1,data2 .. ])
          queryFromBatchedData = "UPSERT INTO " + keyspace + " ([" + batchedData + "])";
          break;
        case DELETE:
          // DELETE FROM <keyspace> WHERE <cond1> OR <cond2> ..
          queryFromBatchedData = "DELETE FROM " + keyspace + " WHERE " + batchedData;
          break;
        default:
          throw new IllegalArgumentException("No Type " + type + " Found");
      }

      return queryFromBatchedData;
    }

    public int getBatchId() {
      return batchId;
    }
  }

}
