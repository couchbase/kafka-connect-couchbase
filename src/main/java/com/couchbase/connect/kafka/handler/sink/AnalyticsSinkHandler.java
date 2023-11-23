/*
 * Copyright 2023 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.analytics.AnalyticsOptions;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.connect.kafka.config.sink.CouchbaseSinkConfig;
import com.couchbase.connect.kafka.util.AnalyticsBatchBuilder;
import com.couchbase.connect.kafka.util.N1qlData;
import com.couchbase.connect.kafka.util.N1qlData.OperationType;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;
import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * This class is used when we have a source and Analytics service as the sink.
 * We can extend this class to deal with different sources.
 */
@Stability.Volatile
public class AnalyticsSinkHandler implements SinkHandler {
  private static final Logger log = LoggerFactory.getLogger(AnalyticsSinkHandler.class);
  protected String bucketName;
  protected int maxRecordsInBatchLimit;
  protected long maxSizeOfRecordsInBytesLimit;
  protected Duration analyticsQueryTimeout;

  private static Pair<String, JsonArray> prepareWhereClauseForDelete(JsonObject documentKeys) {

    List<Object> values = new ArrayList<>();
    String whereClause = documentKeys.getNames().stream().map(key -> {
      values.add(documentKeys.get(key));
      return "`" + key + "`=?";
    }).collect(Collectors.joining(" AND "));
    return Pair.of(whereClause, JsonArray.from(values));
  }

  protected static Pair<String, JsonArray> deleteQuery(String keySpace, JsonObject documentKeys) {
    Pair<String, JsonArray> whereClause = prepareWhereClauseForDelete(documentKeys);
    return Pair.of("DELETE FROM " + keySpace + " WHERE " + whereClause.getLeft() + ";", whereClause.getRight());
  }

  protected static JsonObject getJsonObject(String object) {
    JsonObject node = null;
    try {
      node = JsonObject.fromJson(object);
    } catch (Exception e) {
      log.warn("could not generate analytics statement from node (not json)", e);
    }

    if (node != null && node.isEmpty()) {
      node = null;
      log.warn("could not generate analytics statement from empty node");
    }
    return node;
  }

  protected static String keyspace(String bucketName, String scope, String collection) {
    if (scope.equals("") || collection.equals("")) {
      throw new ConfigException("Missing required configuration for scope and collection.");
    }

    String keySpace = "";
    if (bucketName != null && !bucketName.isEmpty()) {
      keySpace += "`" + bucketName + "`.";
    }
    keySpace += "`" + scope + "`.`" + collection + "`";

    return keySpace;
  }

  @Override
  public void init(SinkHandlerContext context) {
    CouchbaseSinkConfig config = ConfigHelper.parse(CouchbaseSinkConfig.class, context.configProperties());
    maxRecordsInBatchLimit = config.analyticsMaxRecordsInBatch();
    maxSizeOfRecordsInBytesLimit = config.analyticsMaxSizeInBatch().getByteCount();
    analyticsQueryTimeout = config.analyticsQueryTimeout();
    this.bucketName = config.bucket();
  }

  @Override
  public boolean usesKvCollections() {
    return false;
  }

  private String upsertStatement(String keySpace, JsonObject values) {
    return "UPSERT INTO " + keySpace + " ([" + values + "]);";
  }

  @Override
  public SinkAction handle(SinkHandlerParams params) {
    String documentKeys = getDocumentId(params);
    SinkDocument doc = params.document().orElse(null);

    // if bucketName is present then keyspace=bucketName.scopeName.collectionName otherwise keyspace=scopeName.collectionName
    String keySpace = keyspace(bucketName, params.getScopeAndCollection().getScope(), params.getScopeAndCollection().getCollection());

    if (doc != null) {
      final String docContent = new String(doc.content(), UTF_8);
      if (docContent.contains("`")) {
        log.warn("Could not generate Analytics N1QL UPSERT statement with backtick (`) in document content");
        return SinkAction.ignore();
      }

      final JsonObject node = getJsonObject(docContent);
      if (node == null) {
        return SinkAction.ignore();
      }

      String statement = upsertStatement(keySpace, node);

      Mono<?> action = Mono.defer(() ->
          params.cluster()
              .analyticsQuery(statement, analyticsOptions().timeout(analyticsQueryTimeout).parameters(node))
              .map(ReactiveAnalyticsResult::metaData)); // metadata arrival signals query completion

      ConcurrencyHint concurrencyHint = ConcurrencyHint.of(documentKeys);
      return new SinkAction(action, concurrencyHint);
    } else {
      // when doc is null we are deleting the document
      if (documentKeys.contains("`")) {
        log.warn("Could not generate Analytics N1QL DELETE statement with backtick (`) in field name");
        return SinkAction.ignore();
      }

      final JsonObject documentKeysJson = getJsonObject(documentKeys);
      if (documentKeysJson == null) {
        return SinkAction.ignore();
      }

      Pair<String, JsonArray> deleteQuery = deleteQuery(keySpace, documentKeysJson);
      Mono<?> action = Mono.defer(() ->
          params.cluster()
              .analyticsQuery(deleteQuery.getLeft(), analyticsOptions().timeout(analyticsQueryTimeout).parameters(deleteQuery.getRight()))
              .map(ReactiveAnalyticsResult::metaData)); // metadata arrival signals query completion

      ConcurrencyHint concurrencyHint = ConcurrencyHint.of(documentKeys);
      return new SinkAction(action, concurrencyHint);

    }
  }

  @Override
  public List<SinkAction> handleBatch(List<SinkHandlerParams> params) {
    if (params.isEmpty()) {
      return Collections.emptyList();
    }

    AnalyticsBatchBuilder batchBuilder = new AnalyticsBatchBuilder(maxSizeOfRecordsInBytesLimit, maxRecordsInBatchLimit);
    final ReactiveCluster cluster = params.get(0).cluster();

    for (SinkHandlerParams param : params) {
      String documentIds = getDocumentId(param);
      SinkDocument doc = param.document().orElse(null);

      // if bucketName is present then keyspace=bucketName.scopeName.collectionName otherwise keyspace=scopeName.collectionName
      String keySpace = keyspace(bucketName, param.getScopeAndCollection().getScope(), param.getScopeAndCollection().getCollection());

      if (doc != null) {
        //Upsertion Case
        final JsonObject node;
        try {
          node = JsonObject.fromJson(doc.content());
        } catch (Exception e) {
          log.warn("could not generate n1ql statement from node (not json)", e);
          continue;
        }

        if (node.isEmpty()) {
          log.warn("could not generate n1ql statement from node (not json)");
          continue;
        }

        boolean backTicksFoundInKeys = false;
        for (String name : node.getNames()) {
          if (name.contains("`")) {
            backTicksFoundInKeys = true;
            // todo figure out how to escape backticks when generating N1QL statements.
            // For now, bail out to avoid N1QL injection.
            log.warn("could not generate n1ql statement from node with backtick (`) in field name");
            break;
          }
        }

        if (backTicksFoundInKeys) {
          // Ignoring this record
          continue;
        }

        batchBuilder.add(
            new N1qlData(keySpace, node.toString(), OperationType.UPSERT, ConcurrencyHint.of(documentIds))
        );
      } else {
        // when doc is null we are deleting the document
        if (documentIds.contains("`")) {
          log.warn("Could not generate Analytics N1QL DELETE statement with backtick (`) in field name");
          continue;
        }

        final JsonObject documentKeysJson = getJsonObject(documentIds);
        if (documentKeysJson == null) {
          continue;
        }

        // Create Delete Statement
        String deleteCondition = generateDeleteCondition(documentKeysJson);

        batchBuilder.add(
            new N1qlData(keySpace, deleteCondition, OperationType.DELETE, ConcurrencyHint.of(documentIds))
        );
      }
    }

    return batchBuilder.build().stream().map(statement -> new SinkAction(
        Mono.defer(() -> cluster.analyticsQuery(statement, AnalyticsOptions.analyticsOptions().timeout(analyticsQueryTimeout)).map(ReactiveAnalyticsResult::metaData)), ConcurrencyHint.neverConcurrent()
    )).collect(Collectors.toList());
  }

  private String generateDeleteCondition(JsonObject documentKeysJson) {
    String condition = documentKeysJson.getNames().stream().map(key -> {
      Object value = documentKeysJson.get(key);
      if (value instanceof Number) {
        return String.format("%s=%s", key, value);
      } else {
        return String.format("%s=\"%s\"", key, value);
      }
    }).collect(Collectors.joining(" AND "));

    return " ( " + condition + " ) ";
  }

  @Override
  public String toString() {
    return "AnalyticsSinkHandler{" + ", bucketName='" + bucketName + '\'' + '}';
  }

}
