package com.couchbase.connect.kafka.sink;

public enum SubDocumentMode {
    UPSERT("upsert"),
    MERGE("merge"),
    ARRAYINSERT("arrayInsert"),
    ARRAYPREPEND("arrayPrepend"),
    ARRAYAPPEND("arrayAppend"),
    ARRAYINSERTALL("arrayInsertAll"),
    ARRAYPREPENDALL("arrayPrependAll"),
    ARRAYAPPENDALL("arrayAppendAll"),
    ARRAYADDUNIQUE("arrayAddUnique");


    private final String schemaName;

    SubDocumentMode(String schemaName) {
        this.schemaName = schemaName;
    }

    public String schemaName() {
        return schemaName;
    }
}