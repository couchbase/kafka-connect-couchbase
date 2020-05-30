/*
 * Copyright 2020 Couchbase, Inc.
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

import com.couchbase.connect.kafka.sink.DocumentMode;
import com.couchbase.connect.kafka.sink.N1qlMode;
import com.couchbase.connect.kafka.sink.SubDocumentMode;
import com.couchbase.connect.kafka.util.config.annotation.Default;

import java.time.Duration;
import java.util.List;

public interface SinkBehaviorConfig {

  /**
   * Format string to use for the Couchbase document ID (overriding the message key).
   * May refer to document fields via placeholders like ${/path/to/field}
   */
  @Default
  String documentId();

  /**
   * Whether to remove the ID identified by 'couchbase.documentId' from the document before storing in Couchbase.
   */
  @Default("false")
  boolean removeDocumentId();

  /**
   * Setting to indicate an update to the entire document or a sub-document.
   */
  @Default("DOCUMENT")
  DocumentMode documentMode();

  /**
   * JSON Pointer to the property to use as the root for the Couchbase sub-document operation.
   */
  @Default
  String subdocumentPath();

  /**
   * Setting to indicate the type of update to a sub-document.
   */
  @Default("UPSERT")
  SubDocumentMode subdocumentOperation();

  /**
   * Setting to indicate the type of update to use when 'couchbase.documentMode' is 'N1QL'.
   */
  @Default("UPSERT")
  N1qlMode n1qlOperation();

  /**
   * When using the UPDATE_WHERE operation, this is the list of document fields that must match the Kafka message in order for the document to be updated with the remaining message fields.
   * To match against a literal value instead of a message field, use a colon to delimit the document field name and the target value.
   * For example, "type:widget,color" matches documents whose 'type' field  is 'widget' and whose 'color' field matches the 'color' field of the Kafka message.
   */
  @Default
  List<String> n1qlWhereFields();

  /**
   * Whether to add the parent paths if they are missing in the document.
   */
  @Default("true")
  boolean subdocumentCreatePath();

  /**
   * Whether to create the document if it does not exist.
   */
  @Default("true")
  boolean subdocumentCreateDocument();

  /**
   * Document expiration time specified as an integer followed by a time unit (s = seconds, m = minutes, h = hours, d = days).
   * For example, to have documents expire after 30 minutes, set this value to "30m". By default, documents do not expire.
   */
  @Default("0")
  Duration documentExpiration();
}
