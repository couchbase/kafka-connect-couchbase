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

import com.couchbase.connect.kafka.handler.sink.SubDocumentSinkHandler;
import com.couchbase.connect.kafka.util.config.annotation.Default;

/**
 * Config properties used only by {@link SubDocumentSinkHandler}.
 */
public interface SubDocumentSinkHandlerConfig {
  /**
   * JSON Pointer to the property of the Kafka message whose value is
   * the subdocument path to use when modifying the Couchbase document.
   * <p>
   * This property is specific to `SubDocumentSinkHandler`.
   */
  @Default
  String subdocumentPath();

  /**
   * Setting to indicate the type of update to a sub-document.
   * <p>
   * This property is specific to `SubDocumentSinkHandler`.
   */
  @Default("UPSERT")
  Operation subdocumentOperation();

  /**
   * Whether to add the parent paths if they are missing in the document.
   * <p>
   * This property is specific to `SubDocumentSinkHandler`.
   */
  @Default("true")
  boolean subdocumentCreatePath();

  /**
   * This property controls whether to create the document if it does not exist.
   * <p>
   * This property is specific to `SubDocumentSinkHandler`.
   */
  @Default("true")
  boolean subdocumentCreateDocument();

  enum Operation {
    /**
     * Replaces the value at the subdocument path with the Kafka message.
     */
    UPSERT,

    /**
     * Prepend the Kafka message to the array at the subdocument path.
     */
    ARRAY_PREPEND,

    /**
     * Append the Kafka message to the array at the subdocument path.
     */
    ARRAY_APPEND,
  }
}
