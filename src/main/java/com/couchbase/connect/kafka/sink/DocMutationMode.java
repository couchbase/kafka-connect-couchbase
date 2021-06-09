package com.couchbase.connect.kafka.sink;

public enum DocMutationMode {

    /**
     * Replaces the value at the subdocument path with the Kafka message value.
     */
    UPSERT,

    /**
     * Removes the value at the subdocument path.
     */
    REMOVE

}
