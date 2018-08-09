package com.couchbase.connect.kafka.sink;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.N1qlMetrics;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import com.couchbase.connect.kafka.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.deps.io.netty.util.CharsetUtil.UTF_8;

public class N1qlWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(N1qlWriter.class);

    private static final String ID_FIELD = "__id__";

    private final N1qlMode mode;
    private final String conditions;
    private final boolean createDocuments;

    public N1qlWriter(N1qlMode mode, List<String> whereFields, boolean createDocuments) {
        this.mode = mode;
        this.conditions = whereFields == null ? null : conditions(whereFields);
        this.createDocuments = createDocuments;
    }

    public Completable write(final AsyncBucket bucket, final JsonBinaryDocument document, PersistTo persistTo, ReplicateTo replicateTo) {
        if (document == null || document.content() == null) {
            LOGGER.warn("document or document content is null");
            // skip it
            return Completable.complete();
        }

        final JsonObject node;
        try {
            node = JsonObject.fromJson(document.content().toString(UTF_8));
        } catch (IllegalArgumentException e) {
            LOGGER.warn("could not generate n1ql statement from node (not json)", e);
            return Completable.complete();
        }

        if (node.isEmpty()) {
            LOGGER.warn("could not generate n1ql statement from empty node");
            return Completable.complete();
        }

        for (String name : node.getNames()) {
            if (name.contains("`")) {
                // todo figure out how to escape backticks when generating N1QL statements.
                // For now, bail out to avoid N1QL injection.
                LOGGER.warn("could not generate n1ql statement from node with backtick (`) in field name");
                return Completable.complete();
            }
        }

        switch (this.mode) {
            case UPSERT: {
                String statement = upsertStatement(bucket.name(), node);
                JsonObject idObject = JsonObject.create().put(ID_FIELD, document.id());
                N1qlQuery query = N1qlQuery.parameterized(statement, idObject);
                return bucket.query(query).toCompletable();
            }

            case UPDATE_WHERE: {
                String statement = updateWithConditionStatement(bucket.name(), node);
                node.put(ID_FIELD, document.id());
                N1qlQuery query = N1qlQuery.parameterized(statement, node);
                return bucket.query(query).toCompletable();
            }

            case UPDATE: {
                String statement = updateStatement(bucket.name(), node);
                node.put(ID_FIELD, document.id());
                N1qlQuery query = N1qlQuery.parameterized(statement, node);
                if (!createDocuments) {
                    return bucket.query(query).toCompletable();
                }

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
                                    // Document didn't exist, so create it
                                    node.removeKey(ID_FIELD);
                                    String statement = upsertStatement(bucket.name(), node);
                                    JsonObject idObject = JsonObject.create().put(ID_FIELD, document.id());
                                    return bucket.query(N1qlQuery.parameterized(statement, idObject));
                                } else {
                                    return Observable.just(n1qlMetrics);
                                }
                            }
                        })
                        .toCompletable();
            }

            default:
                throw new AssertionError("unrecognized n1ql mode");
        }
    }

    private String upsertStatement(String keySpace, JsonObject values) {
        return "UPSERT INTO `" + keySpace + "`" +
                " (KEY,VALUE) VALUES ($" + ID_FIELD + ", " + values + ")" +
                " RETURNING meta().id;";
    }

    private String updateStatement(String keySpace, JsonObject values) {
        return "UPDATE `" + keySpace + "`" +
                " USE KEYS $" + ID_FIELD +
                " SET " + assignments(values) +
                " RETURNING meta().id;";
    }

    private String updateWithConditionStatement(String keySpace, JsonObject values) {
        return "UPDATE `" + keySpace + "`" +
                " SET " + assignments(values) +
                " WHERE " + conditions +
                " RETURNING meta().id;";
    }

    private static String assignments(JsonObject values) {
        List<String> assignments = new ArrayList<String>();
        for (String name : values.getNames()) {
            assignments.add("`" + name + "` = $" + name);
        }
        return StringUtils.join(assignments, ", ");
    }

    private static String conditions(List<String> fields) {
        List<String> conditions = new ArrayList<String>();

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

        return StringUtils.join(conditions, " AND ");
    }
}
