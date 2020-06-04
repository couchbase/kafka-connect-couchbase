package com.couchbase.connect.kafka.sink;

public enum N1qlMode {
  /**
   * Copy all top-level fields of the Kafka message to the target document,
   * clobbering any existing top-level fields with the same names.
   */
  UPDATE,

  /**
   * Target zero or more documents using a WHERE condition to match fields of the message.
   * <p>
   * Consider the following 3 documents:
   * <pre>
   * {
   *   "id": "airline_1",
   *   "type": "airline",
   *   "name": "airline 1",
   *   "parent_companycode": "AA",
   *   "parent_companyname": "airline inc"
   * }
   * {
   *   "id": "airline_2",
   *   "type": "airline",
   *   "name": "airline 2",
   *   "parent_companycode": "AA",
   *   "parent_companyname": "airline inc"
   * }
   * {
   *   "id": "airline_3",
   *   "type": "airline",
   *   "name": "airline 3",
   *   "parent_companycode": "AA",
   *   "parent_companyname": "airline inc"
   * }
   * </pre>
   * With the UPDATE mode, it would take 3 Kafka messages to update the
   * "parent_companyname" of all the documents, each message using a
   * different ID to target one of the 3 documents.
   * <p>
   * With the UPDATE_WHERE mode, you can send one message and match it on something
   * other than the id. For example, if you configure the plugin like this:
   * <pre>
   * ...
   * "couchbase.document.mode":"N1QL",
   * "couchbase.n1ql.operation":"UPDATE_WHERE",
   * "couchbase.n1ql.where_fields":"type:airline,parent_companycode"
   * ...
   * </pre>
   * the connector will generate an update statement with a WHERE clause
   * instead of ON KEYS. For example, when receiving this Kafka message:
   * <pre>
   * {
   *   "type":"airline",
   *   "parent_companycode":"AA",
   *   "parent_companyname":"airline ltd"
   * }
   * </pre>
   * it would generate a N1QL statement along the lines of:
   * <pre>
   *   UPDATE `keyspace`
   *   SET `parent_companyname` = $parent_companyname
   *   WHERE `type` = $type AND `parent_companycode` = $parent_companycode
   *   RETURNING meta().id
   * </pre>
   */
  UPDATE_WHERE,
}
