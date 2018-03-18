package com.couchbase.connect.kafka.sink;

public enum DocumentMode {
    DOCUMENT("document"),
    SUBDOCUMENT("subdocument");

    private final String schemaName;

    DocumentMode(String schemaName) {
        this.schemaName = schemaName;
    }

    public String schemaName() {
        return schemaName;
    }
}