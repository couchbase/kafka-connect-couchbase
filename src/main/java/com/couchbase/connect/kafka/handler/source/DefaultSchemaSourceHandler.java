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

import com.couchbase.connect.kafka.util.Schemas;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The standard handler. Publishes metadata along with document content.
 *
 * @see Schemas
 * @see RawJsonSourceHandler
 * @see RawJsonWithMetadataSourceHandler
 */
public class DefaultSchemaSourceHandler extends SourceHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSchemaSourceHandler.class);

  @Override
  public CouchbaseSourceRecord handle(SourceHandlerParams params) {
    CouchbaseSourceRecord.Builder builder = CouchbaseSourceRecord.builder();

    // A handler may choose to route the message to any topic.
    // The code shown here sends the message to the topic from the connector configuration.
    // This is optional; if no topic is specified, it defaults to the one from the config.
    builder.topic(params.topic());

    buildKey(params, builder);

    if (!buildValue(params, builder)) {
      // Don't know how to handle this message; skip it!
      // A custom handler may filter the event stream by returning null to skip a message.
      return null;
    }

    return builder.build();
  }

  protected void buildKey(SourceHandlerParams params, CouchbaseSourceRecord.Builder builder) {
    builder.key(Schemas.KEY_SCHEMA, params.documentEvent().key());
  }

  /**
   * @return true to publish the message, or false to skip it
   */
  protected boolean buildValue(SourceHandlerParams params, CouchbaseSourceRecord.Builder builder) {
    final DocumentEvent docEvent = params.documentEvent();
    final DocumentEvent.Type type = docEvent.type();

    final Struct record = new Struct(Schemas.VALUE_DEFAULT_SCHEMA);
    record.put("event", type.schemaName());

    record.put("bucket", docEvent.bucket());
    record.put("partition", docEvent.partition());
    record.put("vBucketUuid", docEvent.partitionUuid());
    record.put("key", docEvent.key());
    record.put("cas", docEvent.cas());
    record.put("bySeqno", docEvent.bySeqno());
    record.put("revSeqno", docEvent.revisionSeqno());

    final MutationMetadata mutation = docEvent.mutationMetadata().orElse(null);
    if (mutation != null) {
      record.put("expiration", mutation.expiry());
      record.put("flags", mutation.flags());
      record.put("lockTime", mutation.lockTime());
      record.put("content", docEvent.content());

    } else if (type != DocumentEvent.Type.DELETION && type != DocumentEvent.Type.EXPIRATION) {
      LOGGER.warn("unexpected event type: {}", type);
      return false;
    }

    builder.value(Schemas.VALUE_DEFAULT_SCHEMA, record);
    return true;
  }
}
