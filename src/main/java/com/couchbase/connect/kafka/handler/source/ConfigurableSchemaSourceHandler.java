/*
 * Copyright 2025 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.connect.kafka.config.source.CouchbaseSourceTaskConfig;
import com.couchbase.connect.kafka.util.SchemaHelper;
import com.couchbase.connect.kafka.util.ScopeAndCollection;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import com.couchbase.connect.kafka.util.config.LookupTable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.couchbase.connect.kafka.util.SchemaHelper.*;

/**
 * This handler enforces a custom schema using the {@link org.apache.kafka.connect.json.JsonConverter}.
 * It only supports documents where the root is a JSON object. Documents that do not match the schema or are not a JSON object will be filtered out
 * The schema is provided in the format of an Avro Schema Record, stored in a Json Object
 * The schema is read from the {@link com.couchbase.connect.kafka.config.source.SchemaConfig#valueSchema()} configuration property
 */
@Stability.Uncommitted
public class ConfigurableSchemaSourceHandler implements SourceHandler{

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurableSchemaSourceHandler.class);
  private LookupTable<ScopeAndCollection, Schema> valueSchemas;

  @Override
  public void init(Map<String, String> configProperties) {
    CouchbaseSourceTaskConfig config = ConfigHelper.parse(CouchbaseSourceTaskConfig.class, configProperties);

    valueSchemas = config.valueSchema()
        .mapKeys(ScopeAndCollection::parse)
        .mapValues(SchemaHelper::parseSchema);
  }

  @Override
  public SourceRecordBuilder handle(SourceHandlerParams params) {
    SourceRecordBuilder builder = new SourceRecordBuilder();

    Schema schemaToUse = valueSchemas.get(ScopeAndCollection.from(params.documentEvent()));

    builder.topic(params.topic());

    // No schema provided is shorthand for String schema
    builder.key(params.documentEvent().key());

    try {
      Struct record = buildStruct(schemaToUse, params.documentEvent().content());

      checkStruct(schemaToUse, record);

      builder.value(schemaToUse, record);
    } catch (Exception e) {
      LOGGER.debug("Document {} doesn't match specified schema. Will not be pushed to Kafka", params.documentEvent().key(), e);
      return null;
    }

    return builder;
  }
}
