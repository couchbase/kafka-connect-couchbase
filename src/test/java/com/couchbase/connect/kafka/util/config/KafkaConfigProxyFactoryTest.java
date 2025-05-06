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

import com.couchbase.connect.kafka.util.config.annotation.ContextDocumentation;
import com.couchbase.connect.kafka.util.config.annotation.Default;
import com.couchbase.connect.kafka.util.config.annotation.EnvironmentVariable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KafkaConfigProxyFactoryTest {
  private static final KafkaConfigProxyFactory factory = new KafkaConfigProxyFactory("foo");

  private enum Color {
    RED, GREEN, BLUE
  }

  private interface TestConfig {
    String stringValue();

    boolean booleanValue();

    int intValue();

    short shortValue();

    double doubleValue();

    List<String> listValue();

    Color enumValue();

    Password passwordValue();

    Duration durationValue();

    DataSize dataSizeValue();

    Class<?> classValue();
  }

  private interface TestConfigWithDefaults {
    @Default("xyzzy")
    String stringValue();

    @Default("true")
    boolean booleanValue();

    @Default("1")
    int intValue();

    @Default("2")
    short shortValue();

    @Default("3.14")
    double doubleValue();

    @Default("a,b,c")
    List<String> listValue();

    @Default
    List<String> emptyListValue();

    @Default("RED")
    Color enumValue();

    @Default("swordfish")
    Password passwordValue();

    @Default("30s")
    Duration durationValue();

    @Default("1k")
    DataSize dataSizeValue();

    @Default("java.lang.String")
    Class<?> classValue();
  }

  @Test
  public void requiredValues() {
    Map<String, String> props = new HashMap<>();
    props.put("foo.string.value", "xyzzy");
    props.put("foo.boolean.value", "true");
    props.put("foo.int.value", "1");
    props.put("foo.short.value", "2");
    props.put("foo.double.value", "3.14");
    props.put("foo.list.value", "a,b,c");
    props.put("foo.enum.value", "RED");
    props.put("foo.password.value", "swordfish");
    props.put("foo.duration.value", "30s");
    props.put("foo.data.size.value", "1k");
    props.put("foo.class.value", "java.lang.String");

    TestConfig config = factory.newProxy(TestConfig.class, props, false);

    assertEquals("xyzzy", config.stringValue());
    assertEquals(true, config.booleanValue());
    assertEquals(1, config.intValue());
    assertEquals(2, config.shortValue());
    assertEquals(3.14, config.doubleValue(), .0001);
    assertEquals(Arrays.asList("a", "b", "c"), config.listValue());
    assertEquals(Color.RED, config.enumValue());
    assertEquals("swordfish", config.passwordValue().value());
    assertEquals(Duration.ofSeconds(30), config.durationValue());
    assertEquals(1024, config.dataSizeValue().getByteCount());
    assertEquals(String.class, config.classValue());

    // just make sure there's no stack overflow
    assertEquals(config, config);
    assertEquals(config.hashCode(), config.hashCode());
    assertEquals(config.toString(), config.toString());
  }

  @Test
  public void defaultValues() throws Exception {
    TestConfigWithDefaults config = factory.newProxy(TestConfigWithDefaults.class, emptyMap(), false);

    assertEquals("xyzzy", config.stringValue());
    assertEquals(true, config.booleanValue());
    assertEquals(1, config.intValue());
    assertEquals(2, config.shortValue());
    assertEquals(3.14, config.doubleValue(), .0001);
    assertEquals(Arrays.asList("a", "b", "c"), config.listValue());
    assertEquals(emptyList(), config.emptyListValue());
    assertEquals(Color.RED, config.enumValue());
    assertEquals("swordfish", config.passwordValue().value());
    assertEquals(Duration.ofSeconds(30), config.durationValue());
    assertEquals(1024, config.dataSizeValue().getByteCount());
    assertEquals(String.class, config.classValue());

    // just make sure there's no stack overflow
    assertEquals(config, config);
    assertEquals(config.hashCode(), config.hashCode());
    assertEquals(config.toString(), config.toString());
  }

  public interface TestConfigWithValidator {
    @Default("hello")
    String stringValue();

    @SuppressWarnings("unused")
    static ConfigDef.Validator stringValueValidator() {
      return PROHIBIT_LETTER_Z;
    }
  }

  private static final ConfigDef.Validator PROHIBIT_LETTER_Z = (name, value) -> {
    if (((String) value).contains("z")) {
      throw new ConfigException(name, value, "The letter 'z' has been outlawed.");
    }
  };

  @Test
  public void customValidator() {
    TestConfigWithValidator config = factory.newProxy(TestConfigWithValidator.class, emptyMap(), false);

    assertEquals("hello", config.stringValue());
    assertThrows(ConfigException.class, () ->
            factory.newProxy(TestConfigWithValidator.class, singletonMap("foo.string.value", "xyzzy"), false),
        "The letter 'z' has been outlawed"
    );
  }

  public interface TestContextualConfigWithValidator {
    @Default("hello")
    @ContextDocumentation(
        contextDescription = "test",
        sampleContext = "foo",
        sampleValue = "bar"
    )
    Contextual<String> stringValue();

    @SuppressWarnings("unused")
    static ConfigDef.Validator stringValueValidator() {
      return PROHIBIT_LETTER_Z;
    }
  }

  @Test
  public void customValidatorIsAppliedToContextualValues() {
    TestContextualConfigWithValidator config = factory.newProxy(
        TestContextualConfigWithValidator.class,
        singletonMap("foo.string.value[bar]", "cheese"),
        false
    );
    assertEquals("hello", config.stringValue().get("unrecognized context"));
    assertEquals("cheese", config.stringValue().get("bar"));

    assertThrows(
        ConfigException.class,
        () -> factory.newProxy(
            TestConfigWithValidator.class,
            singletonMap("foo.string.value[f]", "xyzzy"),
            false
        ),
        "The letter 'z' has been outlawed"
    );

    assertThrows(
        ConfigException.class,
        () -> factory.newProxy(
            TestConfigWithValidator.class,
            singletonMap("foo.string.value[bar]", "xyzzy"),
            false
        ),
        "The letter 'z' has been outlawed"
    );
  }

  public interface BaseConfig {
    @Default("base")
    String baseValue();
  }

  public interface SubclassConfig extends BaseConfig {
    @Default("subclass")
    String subclassValue();
  }

  @Test
  public void inheritedMethodsAreIncluded() {
    SubclassConfig config = factory.newProxy(SubclassConfig.class, emptyMap(), false);
    assertEquals("base", config.baseValue());
    assertEquals("subclass", config.subclassValue());
  }

  private static final String ENVAR_NAME = "SOME_RANDOM_NAME_THAT_SHOULD_NEVER_ACTUALLY_BE_SET";

  public interface EnvarConfig {
    @EnvironmentVariable(ENVAR_NAME)
    Password password();
  }

  @Test
  public void environmentVariable() {
    KafkaConfigProxyFactory factory = new KafkaConfigProxyFactory("foo");
    EnvarConfig config = factory.newProxy(EnvarConfig.class, singletonMap("foo.password", "a"), false);
    assertEquals("a", config.password().value());

    factory.environmentVariableAccessor = singletonMap(ENVAR_NAME, "b")::get;
    assertEquals("b", config.password().value());
  }

  @Test
  public void keyName() {
    KafkaConfigProxyFactory factory = new KafkaConfigProxyFactory("foo");
    assertEquals("foo.string.value", factory.keyName(TestConfig.class, TestConfig::stringValue));
  }
}

