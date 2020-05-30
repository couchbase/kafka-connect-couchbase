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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

abstract class AbstractInvocationHandler implements InvocationHandler {
  private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

  private final String toStringPrefix;

  public AbstractInvocationHandler(String toStringPrefix) {
    this.toStringPrefix = toStringPrefix;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] argsMaybeNull) throws Throwable {
    Object[] args = argsMaybeNull == null ? EMPTY_OBJECT_ARRAY : argsMaybeNull;

    if ("equals".equals(method.getName())
        && args.length == 1
        && method.getParameterTypes()[0].equals(Object.class)) {
      return proxy == args[0];
    }

    if ("hashCode".equals(method.getName()) && args.length == 0) {
      return System.identityHashCode(proxy);
    }

    if ("toString".equals(method.getName()) && args.length == 0) {
      return toStringPrefix + "@" + Integer.toHexString(proxy.hashCode());
    }

    return doInvoke(proxy, method, args);
  }

  protected abstract Object doInvoke(Object proxy, Method method, Object[] args);
}
