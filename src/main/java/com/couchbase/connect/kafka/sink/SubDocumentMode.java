package com.couchbase.connect.kafka.sink;

public enum SubDocumentMode {
    UPSERT,
    ARRAY_INSERT,
    ARRAY_PREPEND,
    ARRAY_APPEND,
    ARRAY_INSERT_ALL,
    ARRAY_PREPEND_ALL,
    ARRAY_APPEND_ALL,
    ARRAY_ADD_UNIQUE;
}