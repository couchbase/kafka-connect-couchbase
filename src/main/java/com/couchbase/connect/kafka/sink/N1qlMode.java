package com.couchbase.connect.kafka.sink;

public enum N1qlMode {
    UPDATE("update"),
    UPSERT("upsert");


    private final String schemaName;

    N1qlMode(String schemaName) {
        this.schemaName = schemaName;
    }

    public String schemaName() {
        return schemaName;
    }
}