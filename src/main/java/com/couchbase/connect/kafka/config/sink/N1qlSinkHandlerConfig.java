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

package com.couchbase.connect.kafka.config.sink;

import com.couchbase.connect.kafka.handler.sink.N1qlSinkHandler;
import com.couchbase.connect.kafka.util.config.annotation.Default;

import java.util.List;

/**
 * Config properties used only by {@link N1qlSinkHandler}.
 */
public interface N1qlSinkHandlerConfig {
  /**
   * The type of update to use when `couchbase.sink.handler` is set to
   * `com.couchbase.connect.kafka.handler.sink.N1qlSinkHandler`.
   * <p>
   * This property is specific to `N1qlSinkHandler`.
   */
  @Default("UPDATE")
  Operation n1qlOperation();

  /**
   * When using the UPDATE_WHERE operation, this is the list of document fields that must match the Kafka message in order for the document to be updated with the remaining message fields.
   * To match against a literal value instead of a message field, use a colon to delimit the document field name and the target value.
   * For example, "type:widget,color" matches documents whose 'type' field  is 'widget' and whose 'color' field matches the 'color' field of the Kafka message.
   * <p>
   * This property is specific to `N1qlSinkHandler`.
   */
  @Default
  List<String> n1qlWhereFields();

  /**
   * Controls whether to create the document if it does not exist.
   * <p>
   * This property is specific to `N1qlSinkHandler`.
   */
  @Default("true")
  boolean n1qlCreateDocument();

  enum Operation {
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
}
