/*
 * Copyright 2018 Couchbase, Inc.
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
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;

public class EnumValidator implements ConfigDef.Validator {
    private final Class<? extends Enum> enumClass;

    public EnumValidator(Class<? extends Enum> enumClass) {
        this.enumClass = enumClass;
    }

    @Override
    public void ensureValid(String name, Object value) {
        try {
            //noinspection unchecked
            Enum.valueOf(enumClass, (String) value);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Bad value '" + value + "' for config key '" + name + "'" +
                    "; must be one of " + Arrays.toString(enumClass.getEnumConstants()));
        }
    }
}
