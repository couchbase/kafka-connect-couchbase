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

package com.couchbase.connect.kafka.example;

import com.couchbase.connect.kafka.handler.source.RawJsonSourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An example of extending {@link RawJsonSourceHandler} to add custom behavior.
 * To use this handler, build this project and move the resulting JAR to
 * the same location as the kafka-connect-couchbase JAR. Then use these connector
 * config properties:
 * <pre>
 * dcp.message.converter.class=com.couchbase.connect.kafka.example.CustomSourceHandler
 * value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
 * </pre>
 * For more customization ideas, please see the source code for {@link RawJsonSourceHandler}
 * and {@link com.couchbase.connect.kafka.handler.source.DefaultSchemaSourceHandler}.
 */
public class CustomSourceHandler extends RawJsonSourceHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomSourceHandler.class);

    @Override
    protected boolean passesFilter(SourceHandlerParams params) {
        // Ignore deletions and expirations instead of sending message with null value.
        // NOTE: another way to achieve this result would be to use a DropIfNullValue transform.
        return params.documentEvent().isMutation();
    }

    @Override
    protected String getTopic(SourceHandlerParams params) {
        // Alter the topic based on document key / content:
        if (params.documentEvent().key().startsWith("xyzzy")) {
            return params.topic() + "-xyzzy";
        }

        // Or use the default topic
        return null;
    }
}
