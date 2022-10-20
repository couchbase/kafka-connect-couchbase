/*
 * Copyright 2020 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonFactory;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonPointer;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.filter.FilteringParserDelegate;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.filter.JsonPointerBasedFilter;
import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.deps.com.fasterxml.jackson.core.filter.TokenFilter.Inclusion.ONLY_INCLUDE_ALL;

/**
 * An example Filter implementation that looks at a field of the document, and skips
 * the document if the field value is not one of the accepted values.
 * <p>
 * <b>CAVEAT:</b> If this filter is used, deletions will not be propagated to Kafka.
 * Document deletion notifications from Couchbase do not include the document content,
 * so there are no field values to filter on.
 * <p>
 * Example config for accepting only documents whose top-level "type" field
 * is either "widget" or "invoice":
 * <pre>
 * couchbase.event.filter=com.couchbase.connect.kafka.example.CustomFilter
 * couchbase.custom.filter.field=/type
 * couchbase.custom.filter.values=widget,invoice
 * </pre>
 */
public class CustomFilter implements Filter {
  private static final Logger log = LoggerFactory.getLogger(CustomFilter.class);

  private static final String FIELD_PROPERTY = "couchbase.custom.filter.field";
  private static final String VALUES_PROPERTY = "couchbase.custom.filter.values";

  private static final ConfigDef configDef = new ConfigDef()
      .define(FIELD_PROPERTY,
          ConfigDef.Type.STRING,
          ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.MEDIUM,
          "A JSON pointer to the field whose value much match one of the allowed values (example: \"/type\").")
      .define(VALUES_PROPERTY,
          ConfigDef.Type.LIST,
          ConfigDef.NO_DEFAULT_VALUE,
          ConfigDef.Importance.MEDIUM,
          "A set of values indicating a document should pass the filter.");

  private static final JsonFactory jsonFactory = new JsonFactory();

  private JsonPointer filterField;
  private Set<String> allowedValues;

  @Override
  public void init(Map<String, String> configProperties) {
    AbstractConfig config = new AbstractConfig(configDef, configProperties);
    filterField = JsonPointer.compile(config.getString(FIELD_PROPERTY));
    allowedValues = new HashSet<>(config.getList(VALUES_PROPERTY));

    log.info("Using custom filtering on field '{}' with allowed values: {}", filterField, allowedValues);
  }

  @Override
  public boolean pass(DocumentEvent event) {
    if (!event.isMutation()) {
      log.debug("Ignoring event {} because field values are not available for {} events.", event, event.type());
      return false;
    }

    try {
      final JsonParser parser = new FilteringParserDelegate(
          jsonFactory.createParser(event.content()), new JsonPointerBasedFilter(filterField), ONLY_INCLUDE_ALL, false);

      if (parser.nextToken() == null) {
        log.debug("Rejecting document '{}' because it has no field at location '{}'.",
            event, filterField);
        return false;
      }

      final String value = parser.getValueAsString();
      if (value == null) {
        log.debug("Rejecting document '{}' because the value matching JSON pointer '{}' is null or non-scalar",
            event, filterField);
        return false;
      }

      if (!allowedValues.contains(value)) {
        log.debug("Rejecting document '{}' because its '{}' field has value '{}' which is not one of the allowed values.",
            event, filterField, value);
        return false;
      }

      log.debug("Accepting document '{}'", event);
      return true;

    } catch (IOException e) {
      log.debug("Rejecting document '{}' because it is not JSON.", event);
      return false;
    }
  }
}
