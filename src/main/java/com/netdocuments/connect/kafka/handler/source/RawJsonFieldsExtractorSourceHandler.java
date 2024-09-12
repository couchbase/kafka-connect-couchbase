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
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.List;

/**
 * This handler extracts specific fields from a Couchbase document. It
 * includes the event and key fields in the output. It also allows
 * filtering based on the key provided a regular expression.
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
public class RawJsonFieldsExtractorSourceHandler extends RawJsonWithMetadataSourceHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawJsonFieldsExtractorSourceHandler.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();
  public static final String FIELDS_CONFIG = "couchbase.custom.handler.nd.fields";
  public static final String TYPES_CONFIG = "couchbase.custom.handler.nd.types";
  public static final String KEY_REGEX_CONFIG = "couchbase.custom.handler.nd.key.regex";
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
          "The regular expression to filter the keys");
  List<String> fields;
  HashSet<String> types;
  Pattern key;

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
  }

  @Override
  public SourceRecordBuilder handle(SourceHandlerParams params) {
    SourceRecordBuilder builder = new SourceRecordBuilder();

    if (!buildValue(params, builder)) {
      return null;
    }

    return builder
        .topic(getTopic(params))
        .key(Schema.STRING_SCHEMA, params.documentEvent().key());
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
    final DocumentEvent docEvent = params.documentEvent();
    final DocumentEvent.Type type = docEvent.type();

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
      byte[] value = objectMapper.writeValueAsBytes(newValue);
      builder.value(null, value);
      return true;
    } catch (JsonProcessingException e) {
      throw new DataException("Failed to serialize data", e);
    }
  }
}
