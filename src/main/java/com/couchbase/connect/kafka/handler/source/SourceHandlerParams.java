/*
 * Copyright 2017 Couchbase, Inc.
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

/**
 * Parameter block for document event handling.
 */
public class SourceHandlerParams {
    private final DocumentEvent documentEvent;
    private final String topic;

    public SourceHandlerParams(DocumentEvent documentEvent, String topic) {
        this.documentEvent = documentEvent;
        this.topic = topic;
    }

    /**
     * Returns the event to be converted to a {@link CouchbaseSourceRecord}.
     */
    public DocumentEvent documentEvent() {
        return documentEvent;
    }

    /**
     * Returns the Kafka topic name from the connector configuration.
     */
    public String topic() {
        return topic;
    }
}
