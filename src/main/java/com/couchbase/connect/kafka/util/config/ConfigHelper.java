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

package com.couchbase.connect.kafka.util.config;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.function.Consumer;

public class ConfigHelper {
  private static final KafkaConfigProxyFactory factory =
      new KafkaConfigProxyFactory("couchbase");

  private ConfigHelper() {
    throw new AssertionError("not instantiable");
  }

  public static ConfigDef define(Class<?> configClass) {
    return factory.define(configClass);
  }

  public static <T> T parse(Class<T> configClass, Map<String, String> props) {
    return factory.newProxy(configClass, props);
  }

  public static <T> String keyName(Class<T> configClass, Consumer<T> methodInvoker) {
    return factory.keyName(configClass, methodInvoker);
  }
}
