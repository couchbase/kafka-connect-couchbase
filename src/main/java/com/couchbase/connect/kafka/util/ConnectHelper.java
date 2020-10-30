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

package com.couchbase.connect.kafka.util;

import org.slf4j.MDC;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConnectHelper {
  private ConnectHelper() {
    throw new AssertionError("not instantiable");
  }

  private static final Pattern taskIdPattern = Pattern.compile("\\|task-(\\d+)");

  /**
   * Returns the connector's task ID, or an empty optional if it could not
   * be determined from the logging context.
   */
  public static Optional<String> getTaskIdFromLoggingContext() {
    // This should be present in Kafka 2.3.0 and later.
    // See https://issues.apache.org/jira/browse/KAFKA-3816
    return Optional.ofNullable(MDC.get("connector.context"))
        .map(context -> {
          Matcher m = taskIdPattern.matcher(context);
          return m.find() ? m.group(1) : null;
        });
  }
}
