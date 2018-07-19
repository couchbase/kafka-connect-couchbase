package com.couchbase.connect.kafka.sink;

import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.N1qlMetrics;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Completable;
import rx.Observable;
import rx.functions.Func1;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;

public class N1qlWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(N1qlWriter.class);

    private N1qlMode mode;

    private String idField = "__id__";

    private boolean createDocuments;

    public N1qlWriter(N1qlMode mode, boolean createDocuments) {
        this.mode = mode;
        this.createDocuments = createDocuments;
    }

    public Completable write(final AsyncBucket bucket, final JsonBinaryDocument document, PersistTo persistTo, ReplicateTo replicateTo) {
        if (document == null || document.content() == null) {
            LOGGER.warn("document or document content is null");
            // skip it
            return Completable.complete();
        }

        final JsonObject node = JsonObject.fromJson(document.content().toString(UTF_8));

        N1qlQuery query = null;
        if (this.mode == N1qlMode.UPDATE) {
            String statement = parseUpdate(bucket.name(), node);

            if (statement == null || statement.isEmpty()) {
                LOGGER.warn("could not generate statement from node " + RedactableArgument.user(node));
                return Completable.complete();
            }

            node.put(idField, document.id());
            query = N1qlQuery.parameterized(statement, node);
        }
        if (this.mode == N1qlMode.UPSERT) {
            String statement = parseUpsert(bucket.name(), node);
            if (statement == null || statement.isEmpty()) {
                LOGGER.warn("could not generate statement from node "  + RedactableArgument.user(node));
                return Completable.complete();
            }

            JsonObject idObject = JsonObject.empty().put(idField, document.id());
            query = N1qlQuery.parameterized(statement, idObject);
        }

        if (mode == N1qlMode.UPDATE && createDocuments) {
            return bucket.query(query)
                    .flatMap(new Func1<AsyncN1qlQueryResult, Observable<N1qlMetrics>>() {
                        @Override
                        public Observable<N1qlMetrics> call(AsyncN1qlQueryResult asyncN1qlQueryResult) {
                            return asyncN1qlQueryResult.info();
                        }
                    })
                    .flatMap(new Func1<N1qlMetrics, Observable<?>>() {
                        @Override
                        public Observable<?> call(N1qlMetrics n1qlMetrics) {
                            if (n1qlMetrics != null && n1qlMetrics.mutationCount() == 0) {
                                String statement = parseUpsert(bucket.name(), node);
                                if (statement == null || statement.isEmpty()) {
                                    LOGGER.warn("could not generate statement from node " + RedactableArgument.user(node));
                                }

                                JsonObject idObject = JsonObject.empty().put(idField, document.id());
                                return bucket.query(N1qlQuery.parameterized(statement, idObject));
                            } else {
                                return Observable.just(n1qlMetrics);
                            }
                        }
                    })
                    .toCompletable();
        }
        else {
            return bucket.query(query).toCompletable();
        }
    }

    private String parseUpdate(String keySpace, JsonObject values) {
        if (values == null || values.equals(JsonObject.empty())) {
            return null;
        }

        StringBuilder statement = new StringBuilder();
        statement.append(String.format("UPDATE `%s` USE KEYS $%s SET ", keySpace, idField));

        for (String name : values.getNames()) {
            statement.append(String.format("`%s` = $%s, ", name, name));
        }

        String result = statement.toString();
        return result.substring(0, result.length() - 2) + " RETURNING meta().id;";
    }

    private String parseUpsert(String keySpace, JsonObject values) {
        if (values == null || values.equals(JsonObject.empty())) {
            return null;
        }

        StringBuilder statement = new StringBuilder();
        statement.append(String.format("UPSERT INTO `%s` (KEY,VALUE) VALUES ($%s, ", keySpace, idField));

        statement.append(values.toString());
        statement.append(") RETURNING meta().id;");

        return statement.toString();
    }
}