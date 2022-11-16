/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.connect.kafka.handler.sink;

import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.couchbase.connect.kafka.config.sink.CouchbaseSinkConfig;
import com.couchbase.connect.kafka.config.sink.N1qlSinkHandlerConfig;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;

public class N1qlSinkHandler implements SinkHandler {
  private static final Logger log = LoggerFactory.getLogger(N1qlSinkHandler.class);

  private static final String ID_FIELD = "__id__";

  private N1qlSinkHandlerConfig.Operation mode;
  protected String conditions;
  protected boolean createDocuments;
  protected String bucketName;

  /**
   * Returns a modified copy of the given map, using the alias property
   * as the default for the canonical property.
   *
   * @param properties the property map to modify.
   * @return a copy of the given map
   */
  static Map<String, String> withAlias(Map<String, String> properties, String canonicalPropertyName, String aliasPropertyName) {
    Map<String, String> copy = new HashMap<>(properties);

    String aliasValue = copy.get(aliasPropertyName);
    String canonicalValue = copy.get(canonicalPropertyName);
    if (canonicalValue == null && aliasValue != null) {
      copy.put(canonicalPropertyName, aliasValue);
    }

    return copy;
  }

  @Override
  public void init(SinkHandlerContext context) {
    // Accept the legacy property "couchbase.create.document" as an alias for "couchbase.n1ql.create.document".
    Map<String, String> properties = withAlias(context.configProperties(),
        ConfigHelper.keyName(N1qlSinkHandlerConfig.class, N1qlSinkHandlerConfig::n1qlCreateDocument),
        "couchbase.create.document");

    CouchbaseSinkConfig config = ConfigHelper.parse(CouchbaseSinkConfig.class, properties);
    this.mode = config.n1qlOperation();
    this.conditions = conditions(config.n1qlWhereFields());
    this.createDocuments = config.n1qlCreateDocument();
    this.bucketName = config.bucket();

    if (mode == N1qlSinkHandlerConfig.Operation.UPDATE_WHERE && conditions.isEmpty()) {
      String keyName = ConfigHelper.keyName(N1qlSinkHandlerConfig.class, N1qlSinkHandlerConfig::n1qlWhereFields);
      throw new ConfigException("Missing required configuration \"" + keyName + "\".");
    }
  }

  @Override
  public SinkAction handle(SinkHandlerParams params) {
    String documentId = getDocumentId(params);
    SinkDocument doc = params.document().orElse(null);
    String keySpace = keyspace(params);

    if (doc == null) {
      return SinkAction.remove(params, params.collection(), documentId);
    }

    final JsonObject node;
    try {
      node = JsonObject.fromJson(doc.content());
    } catch (Exception e) {
      log.warn("could not generate n1ql statement from node (not json)", e);
      return SinkAction.ignore();
    }

    if (node.isEmpty()) {
      log.warn("could not generate n1ql statement from empty node");
      return SinkAction.ignore();
    }

    for (String name : node.getNames()) {
      if (name.contains("`")) {
        // todo figure out how to escape backticks when generating N1QL statements.
        // For now, bail out to avoid N1QL injection.
        log.warn("could not generate n1ql statement from node with backtick (`) in field name");
        return SinkAction.ignore();
      }
    }

    String statement = getStatement(keySpace, node);
    node.put(ID_FIELD, documentId);

    // ReactiveCluster.query is an unholy blend of hot and cold.
    // Make it truly cold by wrapping it with Mono.defer().
    Mono<?> action = Mono.defer(() ->
        params.cluster()
            .query(statement, queryOptions().parameters(node))
            .map(ReactiveQueryResult::metaData)); // metadata arrival signals query completion

    ConcurrencyHint concurrencyHint = mode == N1qlSinkHandlerConfig.Operation.UPDATE
        ? ConcurrencyHint.of(documentId) // UPDATE affects only this document
        : ConcurrencyHint.neverConcurrent(); // UPDATE_WHERE affects unknown documents (usually the sames ones)

    return new SinkAction(action, concurrencyHint);
  }

  /**
   * Returns the target keyspace (bucket + scope + collection)
   * pre-escaped for inclusion in a query statement.
   */
  protected static String keyspace(SinkHandlerParams params) {
    ReactiveCollection c = params.collection();
    String bucket = c.bucketName();
    String scope = defaultIfEmpty(c.scopeName(), "_default");
    String collection = defaultIfEmpty(c.name(), "_default");

    List<String> components = new ArrayList<>();
    components.add(bucket); // always include bucket

    // For compatibility with pre-7.0 servers, omit scope and collection
    // when the keyspace is the default collection.
    boolean defaultCollection =
        scope.equals("_default") && collection.equals("_default");
    if (!defaultCollection) {
      components.add(scope);
      components.add(collection);
    }

    // Escape each component by enclosing in backticks (`),
    // then put a dot (.) in between each component.
    return components.stream()
        .map(it -> "`" + it + "`")
        .collect(Collectors.joining("."));
  }

  private static String defaultIfEmpty(String s, String defaultValue) {
    return s.isEmpty() ? defaultValue : s;
  }

  private String getStatement(String keySpace, JsonObject kafkaMessage) {
    switch (this.mode) {
      case UPDATE_WHERE:
        return updateWithConditionStatement(keySpace, kafkaMessage);
      case UPDATE:
        return createDocuments
            ? mergeStatement(keySpace, kafkaMessage)
            : updateStatement(keySpace, kafkaMessage);
      default:
        throw new AssertionError("unrecognized n1ql mode");
    }
  }

  private String updateStatement(String keySpace, JsonObject values) {
    return "UPDATE " + keySpace +
        " USE KEYS $" + ID_FIELD +
        " SET " + assignments(values) +
        " RETURNING meta().id;";
  }

  private String updateWithConditionStatement(String keySpace, JsonObject values) {
    return "UPDATE " + keySpace +
        " SET " + assignments(values) +
        " WHERE " + conditions +
        " RETURNING meta().id;";
  }

  private String mergeStatement(String keyspace, JsonObject values) {
    return "MERGE INTO " + keyspace + " AS doc" +
        " USING 1 AS o" + // dummy to satisfy the MERGE INTO syntax?
        " ON KEY $" + ID_FIELD +
        " WHEN MATCHED THEN UPDATE SET " + assignments(values, "doc.") +
        " WHEN NOT MATCHED THEN INSERT " + values;
  }

  private static String assignments(JsonObject values) {
    return assignments(values, "");
  }

  private static String assignments(JsonObject values, String prefix) {
    List<String> assignments = new ArrayList<>();
    for (String name : values.getNames()) {
      assignments.add(prefix + "`" + name + "` = $" + name);
    }
    return String.join(", ", assignments);
  }

  private static String conditions(List<String> fields) {
    List<String> conditions = new ArrayList<>();

    for (String name : fields) {
      final String value;

      int colonIndex = name.indexOf(':');
      if (colonIndex != -1) {
        // compare against a string constant (whatever's after the colon)
        value = "'" + name.substring(colonIndex + 1) + "'";
        name = name.substring(0, colonIndex);
      } else {
        value = "$" + name;
      }

      conditions.add("`" + name + "` = " + value);
    }

    return String.join(" AND ", conditions);
  }

  @Override
  public String toString() {
    return "N1qlSinkHandler{" +
        "mode=" + mode +
        ", conditions='" + conditions + '\'' +
        ", createDocuments=" + createDocuments +
        ", bucketName='" + bucketName + '\'' +
        '}';
  }
}

