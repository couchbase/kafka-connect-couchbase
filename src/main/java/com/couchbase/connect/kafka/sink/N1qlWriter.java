package com.couchbase.connect.kafka.sink;


import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.connect.kafka.util.JsonBinaryDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static java.util.Objects.requireNonNull;

public class N1qlWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(N1qlWriter.class);

  private static final String ID_FIELD = "__id__";

  private final N1qlMode mode;
  private final String conditions;
  private final boolean createDocuments;

  public N1qlWriter(N1qlMode mode, List<String> whereFields, boolean createDocuments) {
    this.mode = requireNonNull(mode);
    this.conditions = conditions(whereFields);
    this.createDocuments = createDocuments;
  }

  public Mono<Void> write(final Cluster cluster, final String bucketName, final JsonBinaryDocument document) {
    final JsonObject node;
    try {
      node = JsonObject.fromJson(document.content());
    } catch (IllegalArgumentException e) {
      LOGGER.warn("could not generate n1ql statement from node (not json)", e);
      return Mono.empty();
    }

    if (node.isEmpty()) {
      LOGGER.warn("could not generate n1ql statement from empty node");
      return Mono.empty();
    }

    for (String name : node.getNames()) {
      if (name.contains("`")) {
        // todo figure out how to escape backticks when generating N1QL statements.
        // For now, bail out to avoid N1QL injection.
        LOGGER.warn("could not generate n1ql statement from node with backtick (`) in field name");
        return Mono.empty();
      }
    }

    String statement = getStatement(bucketName, node);
    node.put(ID_FIELD, document.id());
    return cluster.reactive()
        .query(statement, queryOptions().parameters(node))
        .then();
  }

  private String getStatement(String bucketName, JsonObject kafkaMessage) {
    switch (this.mode) {
      case UPDATE_WHERE:
        return updateWithConditionStatement(bucketName, kafkaMessage);
      case UPDATE:
        return createDocuments
            ? mergeStatement(bucketName, kafkaMessage)
            : updateStatement(bucketName, kafkaMessage);
      default:
        throw new AssertionError("unrecognized n1ql mode");
    }
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

  private String mergeStatement(String keyspace, JsonObject values) {
    return "MERGE INTO `" + keyspace + "` AS doc" +
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
}
