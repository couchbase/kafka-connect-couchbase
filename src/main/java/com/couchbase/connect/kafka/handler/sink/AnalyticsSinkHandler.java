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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.analytics.ReactiveAnalyticsResult;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.connect.kafka.config.sink.CouchbaseSinkConfig;
import com.couchbase.connect.kafka.util.config.ConfigHelper;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static com.couchbase.client.java.analytics.AnalyticsOptions.analyticsOptions;


/**
 * This class is used when we have a source and Analytics service as the sink.
 * We can extend this class to deal with different sources.
 */
@Stability.Volatile
public class AnalyticsSinkHandler implements SinkHandler {
  private static final Logger log = LoggerFactory.getLogger(AnalyticsSinkHandler.class);
  protected String bucketName;

  @Override
  public void init(SinkHandlerContext context) {
    CouchbaseSinkConfig config = ConfigHelper.parse(CouchbaseSinkConfig.class, context.configProperties());
    this.bucketName = config.bucket();
  }

  @Override
  public SinkAction handle(SinkHandlerParams params) {
    String documentId = getDocumentId(params);
    SinkDocument doc = params.document().orElse(null);

    // if bucketName is present then keyspace=bucketName.scopeName.collectionName otherwise keyspace=scopeName.collectionName
    String keySpace = keyspace(bucketName, params.getScopeAndCollection().getScope(), params.getScopeAndCollection().getCollection());

    if (doc != null) {
      final JsonObject node;
      try {
        node = JsonObject.fromJson(doc.content());
      } catch (Exception e) {
        log.warn("could not generate analytics statement from node (not json)", e);
        return SinkAction.ignore();
      }

      if (node.isEmpty()) {
        log.warn("could not generate analytics statement from empty node");
        return SinkAction.ignore();
      }

      String statement = upsertStatement(keySpace, node);

      Mono<?> action = Mono.defer(() ->
          params.cluster()
              .analyticsQuery(statement, analyticsOptions().parameters(node))
              .map(ReactiveAnalyticsResult::metaData)); // metadata arrival signals query completion

      ConcurrencyHint concurrencyHint = ConcurrencyHint.of(documentId);
      return new SinkAction(action, concurrencyHint);
    } else {
      // when doc is null we are deleting the document
      String statement = deleteStatement(keySpace, documentId);
      Mono<?> action = Mono.defer(() ->
          params.cluster()
              .analyticsQuery(statement)
              .map(ReactiveAnalyticsResult::metaData)); // metadata arrival signals query completion

      ConcurrencyHint concurrencyHint = ConcurrencyHint.of(documentId);
      return new SinkAction(action, concurrencyHint);

    }
  }

  private String upsertStatement(String keySpace, JsonObject values) {
    return "UPSERT INTO " + keySpace + " ([" + values + "]);";
  }

  private String deleteStatement(String keySpace, String documentId) {
    return "DELETE FROM " + keySpace + " WHERE _id= \"" + documentId + "\" ;";
  }

  @Override
  public String toString() {
    return "AnalyticsSinkHandler{" + ", bucketName='" + bucketName + '\'' + '}';
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

}
