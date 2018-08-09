package com.couchbase.connect.kafka.sink;

public enum N1qlMode {
    /**
     * Target a single document identified by a key, optionally creating it if it does not exist.
     */
    UPDATE,

    /**
     * Target zero or more documents using a WHERE condition to match fields of the message.
     */
    UPDATE_WHERE,

    /**
     * Create or replace a single document identified by a key.
     */
    UPSERT
}
