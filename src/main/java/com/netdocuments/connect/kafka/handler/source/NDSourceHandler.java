/*
 * Copyright 2024 NetDocuments Software, Inc.
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

package com.netdocuments.connect.kafka.handler.source;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import com.couchbase.connect.kafka.handler.source.RawJsonWithMetadataSourceHandler;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.couchbase.connect.kafka.handler.source.SourceRecordBuilder;
import com.couchbase.connect.kafka.util.JsonPropertyExtractor;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This handler extracts specific fields from a Couchbase document. It
 * includes the event and key fields in the output. It also allows
 * filtering based on the key provided a regular expression. If no fields
 * are provided it behaves like the RawJsonWithMetadataSourceHandler.
 * <p>
 * The key of the Kafka message is a String, the ID of the Couchbase document.
 * <p>
 * To use this handler, configure the connector properties like this:
 * 
 * <pre>
 * couchbase.source.handler=com.netdocuments.connect.kafka.source.RawJsonFieldsExtractor
 * value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
 * </pre>
 *
 * @see RawJsonWithMetadataSourceHandler
 */
public class NDSourceHandler extends RawJsonWithMetadataSourceHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NDSourceHandler.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();
  public static final String FIELDS_CONFIG = "couchbase.custom.handler.nd.fields";
  public static final String TYPES_CONFIG = "couchbase.custom.handler.nd.types";
  public static final String KEY_REGEX_CONFIG = "couchbase.custom.handler.nd.key.regex";
  public static final String OUTPUT_FORMAT = "couchbase.custom.handler.nd.output.format";
  private static final ConfigDef configDef = new ConfigDef()
      .define(FIELDS_CONFIG,
          ConfigDef.Type.LIST,
          "",
          ConfigDef.Importance.HIGH,
          "The fields to extract from the document")
      .define(TYPES_CONFIG,
          ConfigDef.Type.LIST,
          "",
          ConfigDef.Importance.LOW,
          "The types to filter on from the type field")
      .define(KEY_REGEX_CONFIG,
          ConfigDef.Type.STRING,
          null,
          ConfigDef.Importance.LOW,
          "The regular expression to filter the keys")
      .define(OUTPUT_FORMAT,
          ConfigDef.Type.STRING,
          null,
          ConfigDef.Importance.LOW,
          "The output format of the message. The only current valid value is 'cloudeevent' anything else designates the default format");
  List<String> fields;
  HashSet<String> types;
  Pattern key;
  Boolean cloudevent = false;

  @Override
  public void init(Map<String, String> configProperties) {
    super.init(configProperties);
    AbstractConfig config = new AbstractConfig(configDef, configProperties);
    fields = config.getList(FIELDS_CONFIG);
    types = new HashSet<String>(config.getList(TYPES_CONFIG));
    if (!types.isEmpty()) {
      fields.add("type");
    }
    String keyRaw = configProperties.get(KEY_REGEX_CONFIG);
    key = keyRaw == null ? null : Pattern.compile(keyRaw.toLowerCase(), Pattern.CASE_INSENSITIVE);
    String cloudEventRaw = config.getString(OUTPUT_FORMAT);
    if (cloudEventRaw != null)
      cloudevent = cloudEventRaw.equals("cloudevent");
  }

  @Override
  public SourceRecordBuilder handle(SourceHandlerParams params) {
    SourceRecordBuilder builder = new SourceRecordBuilder();
    if (cloudevent) {
      builder.headers().add("ce_specversion", new SchemaAndValue(Schema.STRING_SCHEMA, "1.0"));
      builder.headers().add("content-type", new SchemaAndValue(Schema.STRING_SCHEMA, "application/cloudevents"));
    }
    if (!buildValue(params, builder)) {
      return null;
    }

    return builder
        .topic(getTopic(params))
        .key(Schema.STRING_SCHEMA, params.documentEvent().key());
  }

  private static final byte[] dataFieldNameBytes = ",\"data\":".getBytes(UTF_8);

  protected byte[] withCloudEvent(byte[] value, DocumentEvent documentEvent) {
    final Map<String, Object> data = new HashMap<String, Object>();
    data.put("specversion", "1.0");
    data.put("id", documentEvent.key() + "-" + documentEvent.revisionSeqno());
    data.put("type", "com.netdocuments.ndserver." + documentEvent.bucket() + "." + documentEvent.type().schemaName());
    data.put("source", "netdocs://ndserver/" + documentEvent.bucket());
    data.put("time", OffsetDateTime.now().toString());
    data.put("datacontenttype", "application/json;charset=utf-8");
    data.put("partitionkey", documentEvent.key());
    data.put("traceparent", UUID.randomUUID().toString());
    byte[] dataBytes;
    try {
      dataBytes = objectMapper.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to serialize data", e);
    }
    ByteArrayBuilder result = new ByteArrayBuilder(value.length + dataFieldNameBytes.length + dataBytes.length)
        .append(dataBytes, dataBytes.length - 1)
        .append(dataFieldNameBytes)
        .append(value)
        .append((byte) '}');
    return result.build();
  }

  protected byte[] convertToBytes(Map<String, Object> value, DocumentEvent docEvent) {
    /*
     * {
     * "specversion": "1.0",
     * "id": "3a20f7c4-5f94-44e8-81eb-2a4e057bed82",
     * "type": "com.netdocuments.documents.metadata.document.updated.v1",
     * "source": "netdocs://documents/metadata",
     * "datacontenttype": "application/json;charset=utf-8",
     * "time": "2024-09-03T19:42:41.4741209Z",
     * "traceparent": "21168e25-34af-41e9-92f2-58f6e239a605",
     * "partitionkey": "0068-8919-8786",
     * "data": { }
     */
    if (!cloudevent) {
      try {
        return objectMapper.writeValueAsBytes(value);
      } catch (JsonProcessingException e) {
        throw new DataException("Failed to serialize data", e);
      }
    }
    try {
      return withCloudEvent(objectMapper.writeValueAsBytes(value), docEvent);
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to serialize data", e);
    }
  }

  protected boolean buildValue(SourceHandlerParams params, SourceRecordBuilder builder) {
    if (!super.buildValue(params, builder)) {
      return false;
    }
    if (key != null) {
      if (!key.matcher(params.documentEvent().key()).matches()) {
        LOGGER.info("key {} does not match pattern `{}`", params.documentEvent().key(), key.pattern());
        return false;
      }
    }
    if (fields.isEmpty()) {
      super.buildValue(params, builder);
      return true;
    }

    final DocumentEvent docEvent = params.documentEvent();
    final DocumentEvent.Type type = docEvent.type();

    if (fields.size() == 1 && fields.get(0).equals("*")) {
      // If the fields list contains only "*", then we want to extract all fields
      // but not act like RawJasonWithMetadataSourceHandler
      switch (type) {
        case EXPIRATION:
        case DELETION:
          final Map<String, Object> newValue = new HashMap<String, Object>();
          newValue.put("event", type.schemaName());
          newValue.put("key", docEvent.key());
          try {
            byte[] value = convertToBytes(newValue, docEvent);
            builder.value(null, value);
            return true;
          } catch (DataException e) {
            throw e;
          }

        case MUTATION:
          if (params.noValue()) {
            builder.value(null, convertToBytes(null, docEvent));
            return true;
          }

          final byte[] document = docEvent.content();
          if (!isValidJson(document)) {
            LOGGER.warn("Skipping non-JSON document: bucket={} key={}", docEvent.bucket(), docEvent.qualifiedKey());
            return false;
          }
          if (cloudevent)
            builder.value(null, withCloudEvent(document, docEvent));
          else
            builder.value(null, document);
          return true;

        default:
          LOGGER.warn("unexpected event type {}", type);
          return false;
      }
    }

    final byte[] content = docEvent.content();
    final Map<String, Object> newValue;
    if (type == DocumentEvent.Type.DELETION) {
      newValue = new HashMap<String, Object>();
      newValue.put("event", type.schemaName());
      newValue.put("key", docEvent.key());
    } else if (type == DocumentEvent.Type.EXPIRATION) {
      newValue = new HashMap<String, Object>();
      newValue.put("event", type.schemaName());
      newValue.put("key", docEvent.key());
    } else if (type == DocumentEvent.Type.MUTATION) {
      try {
        newValue = JsonPropertyExtractor.extract(new ByteArrayInputStream(content),
            fields.toArray(new String[fields.size()]));
      } catch (Exception e) {
        LOGGER.error("Error while extracting fields from document", e);
        return false;
      }
      if (!types.isEmpty()) {
        if (newValue.containsKey("type") && !types.contains(newValue.get("type"))) {
          LOGGER.info("type {} not in types {}", newValue.get("type"), types);
          return false;
        }
      }
      newValue.put("event", type.schemaName());
      newValue.put("key", docEvent.key());
    } else {
      LOGGER.warn("unexpected event type {}", type);
      return false;
    }

    try {
      builder.value(null, convertToBytes(newValue, docEvent));
      return true;
    } catch (DataException e) {
      throw new DataException("Failed to serialize data", e);
    }
  }
}
