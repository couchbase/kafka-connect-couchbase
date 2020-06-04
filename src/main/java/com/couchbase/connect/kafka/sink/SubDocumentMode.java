package com.couchbase.connect.kafka.sink;

public enum SubDocumentMode {
  /**
   * Replaces the value at the subdocument path with the Kafka message.
   */
  UPSERT,

  /**
   * Prepend the Kafka message to the array at the subdocument path.
   */
  ARRAY_PREPEND,

  /**
   * Append the Kafka message to the array at the subdocument path.
   */
  ARRAY_APPEND,
}
