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

import com.couchbase.connect.kafka.converter.Converter;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Allows a legacy Converter implementation to act like a SourceHandler.
 */
@SuppressWarnings("deprecation")
public class LegacySourceHandlerAdapter extends SourceHandler {
    private final Converter converter;

    public LegacySourceHandlerAdapter(Converter converter) {
        this.converter = converter;
    }

    @Override
    public CouchbaseSourceRecord handle(SourceHandlerParams params) {
        SourceRecord r = converter.convert(
                params.documentEvent().rawDcpEvent(),
                params.documentEvent().bucket(),
                params.topic());

        return r == null ? null :
                new CouchbaseSourceRecord(
                        r.topic(), r.kafkaPartition(),
                        r.keySchema(), r.key(),
                        r.valueSchema(), r.value(),
                        r.timestamp());
    }
}
