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

import com.couchbase.connect.kafka.transform.DeserializeJson;
import com.couchbase.connect.kafka.util.JsonHelper;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This handler propagates JSON documents from Couchbase to Kafka verbatim with no schema.
 * That is, the Kafka message will be identical to the content of the Couchbase document.
 * Deletions are propagated as a message with a {@code null} value.
 * Modifications to non-JSON documents are not propagated.
 * <p>
 * The key of the Kafka message is the ID of the Couchbase document.
 * <p>
 * The value of the generated ConnectRecord is a byte array.
 * If there are no downstream transforms, configure the connector like this
 * for maximum efficiency:
 * <pre>
 * couchbase.source.handler=com.couchbase.connect.kafka.handler.source.RawJsonSourceHandler
 * value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
 * </pre>
 * If you wish to use Single Message Transforms with this handler, the first transform
 * must be {@link DeserializeJson} to convert the
 * byte array to a Map that downstream transforms can work with. Like this:
 * <pre>
 * couchbase.source.handler=com.couchbase.connect.kafka.handler.source.RawJsonSourceHandler
 * value.converter=org.apache.kafka.connect.json.JsonConverter
 * value.converter.schemas.enable=false
 * transforms=deserializeJson,ignoreDeletes,addField
 * transforms.deserializeJson.type=com.couchbase.connect.kafka.transform.DeserializeJson
 * transforms.ignoreDeletes.type=com.couchbase.connect.kafka.transform.DropIfNullValue
 * transforms.addField.type=org.apache.kafka.connect.transforms.InsertField$Value
 * transforms.addField.static.field=magicWord
 * transforms.addField.static.value=xyzzy
 * </pre>
 *
 * @see RawJsonWithMetadataSourceHandler
 */
public class RawJsonSourceHandler implements SourceHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawJsonSourceHandler.class);

  protected static boolean isValidJson(byte[] bytes) {
    return JsonHelper.isValidJson(bytes);
  }

  @Override
  public SourceRecordBuilder handle(SourceHandlerParams params) {
    final SourceRecordBuilder builder = new SourceRecordBuilder();

    if (!passesFilter(params)) {
      return null;
    }

    if (!buildValue(params, builder)) {
      return null;
    }

    return builder.topic(getTopic(params))
        .key(Schema.STRING_SCHEMA, params.documentEvent().key());
  }

  protected boolean passesFilter(SourceHandlerParams params) {
    return true;
  }

  protected boolean buildValue(SourceHandlerParams params, SourceRecordBuilder builder) {
    final DocumentEvent docEvent = params.documentEvent();
    final DocumentEvent.Type type = docEvent.type();

    switch (type) {
      case EXPIRATION:
      case DELETION:
        builder.value(null, null);
        return true;

      case MUTATION:
        if (params.noValue()) {
          builder.value(null, null);
          return true;
        }

        final byte[] document = docEvent.content();
        if (!docEvent.isJson()) {
          LOGGER.warn("Skipping non-JSON document: bucket={} key={}", docEvent.bucket(), docEvent.qualifiedKey());
          return false;
        }

        builder.value(null, document);
        return true;

      default:
        LOGGER.warn("unexpected event type {}", type);
        return false;
    }
  }

  protected String getTopic(SourceHandlerParams params) {
    // Alter the topic based on document key / content:
    //
    // if (params.documentEvent().key().startsWith("xyzzy")) {
    //     return params.topic() + "-xyzzy";
    // }

    // Or use the default topic
    return null;
  }
}
