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

package com.couchbase.connect.kafka.config.common;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.logging.RedactionLevel;
import com.couchbase.connect.kafka.util.config.annotation.Default;

public interface LoggingConfig {
  /**
   * Determines which kinds of sensitive log messages from the Couchbase connector
   * will be tagged for later redaction by the Couchbase log redaction tool.
   * NONE = no tagging; PARTIAL = user data is tagged; FULL = user, meta, and system data is tagged.
   */
  @Default("NONE")
  RedactionLevel logRedaction();

  /**
   * If true, document lifecycle milestones will be logged at INFO level
   * instead of DEBUG. Enabling this feature lets you watch documents
   * flow through the connector. Disabled by default because it generates
   * many log messages.
   *
   * @since 4.0.5
   */
  @Stability.Uncommitted
  @Default("false")
  boolean logDocumentLifecycle();
}
