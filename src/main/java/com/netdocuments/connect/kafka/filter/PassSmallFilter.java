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

package com.netdocuments.connect.kafka.filter;

import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Filter that skips documents above a certain size. Defaults
 * to 100KB. Configuration value is in bytes.
 * 
 * <pre>
 * couchbase.event.filter=com.couchbase.connect.kafka.filter.PassSmallFilter
 * couchbase.custom.filter.size=1024
 * </pre>
 */
public class PassSmallFilter implements Filter {
  private static final Logger log = LoggerFactory.getLogger(PassSmallFilter.class);

  private static final String SIZE_PROPERTY = "couchbase.custom.filter.size";

  private static final ConfigDef configDef = new ConfigDef()
      .define(SIZE_PROPERTY,
          ConfigDef.Type.LONG,
          1024 * 100,
          ConfigDef.Importance.MEDIUM,
          "The maximum size of the document to pass");

  private long size;

  @Override
  public void init(Map<String, String> configProperties) {
    AbstractConfig config = new AbstractConfig(configDef, configProperties);
    size = config.getLong(SIZE_PROPERTY);

    log.info("Filtering messages larger than '{}'", size);
  }

  @Override
  public boolean pass(DocumentEvent event) {
    try {
      if (event.content().length > size) {
        log.debug("Rejecting document '{}' because it is too large.", event);
        return false;
      }
    } catch (Exception e) {
      log.error("Error while filtering document '{}'", event, e);
    }
    return true;
  }
}
