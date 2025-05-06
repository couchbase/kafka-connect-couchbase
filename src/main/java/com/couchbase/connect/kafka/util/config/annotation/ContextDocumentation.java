/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka.util.config.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * When applied to a config method that returns {@link com.couchbase.connect.kafka.util.config.Contextual},
 * describes the meaning of the context string.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ContextDocumentation {
  /**
   * Describes the context's meaning. Should start with an article, if appropriate.
   * Example: "the source topic".
   */
  String contextDescription();

  /**
   * A sample context to include in generated documentation.
   */
  String sampleContext();

  /**
   * A sample property value to include in generated documentation.
   */
  String sampleValue();

}
