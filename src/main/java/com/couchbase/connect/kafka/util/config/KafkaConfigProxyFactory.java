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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.connect.kafka.util.config.annotation.ContextDocumentation;
import com.couchbase.connect.kafka.util.config.annotation.Default;
import com.couchbase.connect.kafka.util.config.annotation.Dependents;
import com.couchbase.connect.kafka.util.config.annotation.DisplayName;
import com.couchbase.connect.kafka.util.config.annotation.EnvironmentVariable;
import com.couchbase.connect.kafka.util.config.annotation.Group;
import com.couchbase.connect.kafka.util.config.annotation.Importance;
import com.couchbase.connect.kafka.util.config.annotation.Width;
import com.github.therapi.runtimejavadoc.ClassJavadoc;
import com.github.therapi.runtimejavadoc.MethodJavadoc;
import com.github.therapi.runtimejavadoc.OtherJavadoc;
import com.github.therapi.runtimejavadoc.RuntimeJavadoc;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.couchbase.connect.kafka.util.config.HtmlRenderer.htmlToPlaintext;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Given a config interface, generates a matching Kafka ConfigDef.
 * <p>
 * Given a config interface and a set of config properties, returns an
 * implementation of the interface that can be used to access the
 * config properties in a type-safe way.
 * <p>
 * A "config interface" is any interface containing only zero-arg methods
 * whose return type is one of:
 * <ul>
 *   <li>String
 *   <li>boolean
 *   <li>int
 *   <li>short
 *   <li>long
 *   <li>double
 *   <li>Class
 *   <li>List&lt;String&gt;
 *   <li>{@link Contextual}&lt;T&gt; (where T is any other type in this list)
 *   <li>{@link Password}
 *   <li>{@link Duration}
 *   <li>{@link DataSize}
 *   <li>any enum
 * </ul>
 * Support for additional types can be added by calling {@link #register(Class, CustomTypeHandler)}.
 * <p>
 * Each interface method corresponds to a Kafka config key. The return type of the method
 * determines the type of the config key. Other config key attributes are inferred
 * from the method, or can be made explicit by annotating the method
 * with one of the annotations in {@link com.couchbase.connect.kafka.util.config.annotation}.
 */
public class KafkaConfigProxyFactory {
  private static final Logger log = LoggerFactory.getLogger(KafkaConfigProxyFactory.class);

  protected final String prefix;
  protected final Map<Class<?>, CustomTypeHandler<?>> customTypeMap = new HashMap<>();
  protected final Map<Class<?>, ConfigDef.Type> javaClassToKafkaType = new HashMap<>();

  // visible for testing
  Function<String, String> environmentVariableAccessor = System::getenv;

  public interface CustomTypeHandler<T> {
    T valueOf(String value);

    default ConfigDef.Validator validator() {
      return null;
    }

    default ConfigDef.Recommender recommender() {
      return null;
    }
  }

  /**
   * @param prefix The string to prepend to all generated config property names.
   */
  public KafkaConfigProxyFactory(String prefix) {
    // make sure prefix is either empty, or ends with dot.
    this.prefix = prefix.isEmpty()
        ? ""
        : (prefix.endsWith(".") ? prefix : prefix + ".");

    initTypeMap();

    register(Duration.class, new CustomTypeHandler<Duration>() {
      @Override
      public Duration valueOf(String value) {
        return DurationParser.parseDuration(value);
      }

      @Override
      public ConfigDef.Validator validator() {
        return new DurationValidator();
      }
    });

    register(DataSize.class, new CustomTypeHandler<DataSize>() {
      @Override
      public DataSize valueOf(String value) {
        return DataSizeParser.parseDataSize(value);
      }

      @Override
      public ConfigDef.Validator validator() {
        return new DataSizeValidator();
      }
    });
  }

  public <T> KafkaConfigProxyFactory register(Class<T> customType, CustomTypeHandler<T> handler) {
    customTypeMap.put(customType, handler);
    javaClassToKafkaType.put(customType, ConfigDef.Type.STRING);
    return this;
  }

  /**
   * Returns a Kafka ConfigDef whose config keys match the methods of the
   * given interface.
   */
  public <T> ConfigDef define(Class<T> configInterface) {
    return define(configInterface, new ConfigDef(), emptyMap());
  }

  /**
   * Returns the given Kafka ConfigDef, augmented with config keys from
   * the given interface.
   * <p>
   * Also adds config keys for contextual properties in the given property map
   * that have a base name matching one of the property names derived from the interface.
   */
  public <T> ConfigDef define(
      Class<T> configInterface,
      ConfigDef def,
      Map<String, String> properties
  ) {
    Map<String, List<String>> index = baseNameToSubscriptedName(properties.keySet());

    for (Method method : configInterface.getMethods()) {
      if (Modifier.isStatic(method.getModifiers())) {
        continue;
      }

      validateReturnType(method);

      ConfigDef.Type kafkaType = getKafkaType(method);
      Object defaultValue = getDefaultValue(method);
      ConfigDef.Validator validator = getValidator(method);
      ConfigDef.Importance importance = getImportance(method);
      String documentation = getDocumentation(method);
      String group = getGroup(method);
      int orderInGroup = getOrderInGroup(method);
      ConfigDef.Width width = getWidth(method);
      String displayName = getDisplayName(method);
      List<String> dependents = getDependents(method);
      ConfigDef.Recommender recommender = getRecommender(method);

      String baseName = getConfigKeyName(method);
      List<String> subscriptedNames = index.getOrDefault(baseName, emptyList());
      if (!subscriptedNames.isEmpty() && !method.getReturnType().equals(Contextual.class)) {
        throw new ConfigException("Invalid config property '" + subscriptedNames.get(0) + "' ; property '" + baseName + "' does not support contextual overrides (square brackets)");
      }

      List<String> keysToDefine = new ArrayList<>();
      keysToDefine.add(baseName);
      keysToDefine.addAll(subscriptedNames);

      keysToDefine.forEach(configKeyName ->
          def.define(
              new ConfigDef.ConfigKey(
                  configKeyName,
                  kafkaType,
                  defaultValue,
                  validator,
                  importance,
                  documentation,
                  group,
                  orderInGroup,
                  width,
                  displayName,
                  dependents,
                  recommender,
                  false
              )
          )
      );
    }

    return def;
  }

  /**
   * Given a set of property names, returns a map where the keys are the base names
   * and the values are the associated subscripted names (with square brackets).
   * <p>
   * For example, if given:
   * <pre>
   * Set.of(
   *   "foo",
   *   "foo[a]",
   *   "foo[b]",
   *   "bar[x]",
   *   "zot"
   * )
   * </pre>
   * returns:
   * <pre>
   * Map.of(
   *   "foo", List.of("foo[a]", "foo[b]"),
   *   "bar", List.of("bar[x]")
   * )
   * </pre>
   */
  static Map<String, List<String>> baseNameToSubscriptedName(Set<String> propertyNames) {
    Map<String, List<String>> result = new LinkedHashMap<>();
    propertyNames.forEach(key -> {
      String subscript = getSubscript(key);

      if (subscript != null) {
        String base = key.substring(0, key.indexOf('['));
        result.computeIfAbsent(base, k -> new ArrayList<>()).add(key);
      }
    });
    return result;
  }

  /**
   * Returns in implementation of the given config interface
   * backed by the given properties.
   * <p>
   * Logs the config.
   */
  public <T> T newProxy(Class<T> configInterface, Map<String, String> properties) {
    return newProxy(configInterface, properties, true);
  }

  /**
   * Returns in implementation of the given config interface
   * backed by the given properties.
   *
   * @param doLog whether to log the config.
   */
  public <T> T newProxy(Class<T> configInterface, Map<String, String> properties, boolean doLog) {
    ConfigDef configDef = define(configInterface, new ConfigDef(), properties);
    ConcreteKafkaConfig kafkaConfig = new ConcreteKafkaConfig(configDef, properties, doLog);

    return configInterface.cast(
        Proxy.newProxyInstance(
            configInterface.getClassLoader(),
            new Class[]{configInterface},
            new AbstractInvocationHandler(configInterface.getName()) {
              @Override
              protected Object doInvoke(Object proxy, Method method, Object[] args) {
                String configKeyName = getConfigKeyName(method);

                if (method.getReturnType().equals(Contextual.class)) {
                  return getContextual(method, configKeyName, properties, kafkaConfig);
                }

                Object result = getValueFromEnvironmentVariable(configKeyName, method)
                    .orElse(kafkaConfig.get(configKeyName));
                return postProcessValue(method, configKeyName, result);
              }
            }));
  }

  private static boolean isParameterizedType(Type t, Class<?> raw, Class<?>... params) {
    if (!(t instanceof ParameterizedType)) {
      return false;
    }

    ParameterizedType parameterizedType = (ParameterizedType) t;
    if (!parameterizedType.getRawType().equals(raw)) {
      return false;
    }

    Type[] actualParams = parameterizedType.getActualTypeArguments();

    if (params.length != actualParams.length) {
      return false;
    }

    return hasParameters(t, params);
  }

  private static boolean isListOfStrings(Type t) {
    return isParameterizedType(t, List.class, String.class);
  }

  private Contextual<?> getContextual(
      Method method,
      String configKeyBaseName,
      Map<String, String> properties,
      ConcreteKafkaConfig kafkaConfig
  ) {
    Map<String, Object> contextToValue = new LinkedHashMap<>();

    Type contextualValueJavaType = contexualValueType(method.getGenericReturnType());

    Class<?> contextualValueJavaClass = contextualValueClass(method.getGenericReturnType());
    if (contextualValueJavaClass == null) {
      throw new IllegalArgumentException("Contextual value type must be a List<String> or a raw type, but got: " + contextualValueJavaType);
    }

    Map<String, List<String>> index = baseNameToSubscriptedName(properties.keySet());
    index.getOrDefault(configKeyBaseName, emptyList()).forEach(subscriptedName -> {
      String subscript = requireNonNull(getSubscript(subscriptedName), "oops, expected this key to have a subscript: " + subscriptedName);
      contextToValue.put(subscript, getAndPostProcess(kafkaConfig, contextualValueJavaClass, subscriptedName));
    });

    Object fallbackValue = getAndPostProcess(kafkaConfig, contextualValueJavaClass, configKeyBaseName);
    return new Contextual<>(configKeyBaseName, fallbackValue, contextToValue);
  }

  private Object getAndPostProcess(ConcreteKafkaConfig kafkaConfig, Class<?> valueClass, String configKeyName) {
    Object value = kafkaConfig.get(configKeyName);
    return postProcessValue(valueClass, configKeyName, value);
  }

  /**
   * Given a string like {@code "foo[bar]"}, returns the substring between the square brackets.
   * Returns null if there are no square brackets.
   *
   * @throws IllegalArgumentException If there are brackets, but not at the end of the string.
   */
  private static @Nullable String getSubscript(String s) {
    int index = s.indexOf('[');
    if (index == -1 || !s.endsWith("]")) {
      if (s.contains("[") || s.contains("]")) {
        throw new IllegalArgumentException("Invalid property name. Square brackets can only appear at end of name to enclose context (like \"some.property[some.context]\"), but got: " + s);
      }
      return null;
    }
    return s.substring(index + 1, s.length() - 1);
  }

  /**
   * Returns the name of the config key associated with the method invoked
   * by the given consumer.
   * <p>
   * Example usage:
   * <pre>
   * String name = proxyFactory.keyName(MyConfig.class, MyConfig::myProperty);
   * </pre>
   *
   * @param configInterface the config interface to inspect
   * @param methodInvoker accepts an implementation of the specified interface
   * and calls the method whose name you want to know
   */
  public <T> String keyName(Class<T> configInterface, Consumer<T> methodInvoker) {
    try {
      T instance = newProxyForKeyNames(configInterface);
      methodInvoker.accept(instance);
      throw new IllegalArgumentException("Consumer should have invoked a method of the config interface.");

    } catch (KeyNameHolderException e) {
      return e.name;
    }
  }

  /**
   * Returns an implementation whose methods all throw an exception
   * that holds the name of the config key associated with the method.
   */
  protected <T> T newProxyForKeyNames(Class<T> configInterface) {
    return configInterface.cast(
        Proxy.newProxyInstance(
            configInterface.getClassLoader(),
            new Class[]{configInterface},
            new AbstractInvocationHandler(configInterface.getName()) {
              @Override
              protected Object doInvoke(Object proxy, Method method, Object[] args) {
                throw new KeyNameHolderException(getConfigKeyName(method));
              }
            }));
  }

  protected static class KeyNameHolderException extends RuntimeException {
    private final String name;

    public KeyNameHolderException(String name) {
      super(name);
      this.name = requireNonNull(name);
    }
  }

  protected Object postProcessValue(Method method, String keyName, Object value) {
    return postProcessValue(method.getReturnType(), keyName, value);
  }

  protected Object postProcessValue(Class<?> javaType, String keyName, Object value) {
    try {
      CustomTypeHandler<?> customTypeHandler = customTypeMap.get(javaType);
      if (customTypeHandler != null) {
        return customTypeHandler.valueOf((String) value);
      }

      if (javaType.isEnum()) {
        return parseEnum(javaType, (String) value);
      }

      return value;

    } catch (Exception e) {
      throw new ConfigException(keyName, value, e.getMessage());
    }
  }

  protected String getEnv(String environmentVariableName) {
    return environmentVariableAccessor.apply(environmentVariableName);
  }

  protected Optional<Object> getValueFromEnvironmentVariable(String configKeyName, Method method) {
    String envarName = getEnvironmentVariableName(method).orElse(null);
    if (envarName != null) {
      String envarValue = getEnv(envarName);
      if (envarValue != null) {
        log.info("Reading value for '{}' from environment variable '{}'", configKeyName, envarName);
        return Optional.of(ConfigDef.parseType(configKeyName, envarValue, getKafkaType(method)));
      } else {
        log.debug("Environment variable '{}' not set.", envarName);
      }
    }
    return Optional.empty();
  }

  protected void initTypeMap() {
    javaClassToKafkaType.put(Boolean.class, ConfigDef.Type.BOOLEAN);
    javaClassToKafkaType.put(Boolean.TYPE, ConfigDef.Type.BOOLEAN);
    javaClassToKafkaType.put(String.class, ConfigDef.Type.STRING);
    javaClassToKafkaType.put(Integer.class, ConfigDef.Type.INT);
    javaClassToKafkaType.put(Integer.TYPE, ConfigDef.Type.INT);
    javaClassToKafkaType.put(Short.class, ConfigDef.Type.SHORT);
    javaClassToKafkaType.put(Short.TYPE, ConfigDef.Type.SHORT);
    javaClassToKafkaType.put(Long.class, ConfigDef.Type.LONG);
    javaClassToKafkaType.put(Long.TYPE, ConfigDef.Type.LONG);
    javaClassToKafkaType.put(Double.class, ConfigDef.Type.DOUBLE);
    javaClassToKafkaType.put(Double.TYPE, ConfigDef.Type.DOUBLE);
    javaClassToKafkaType.put(List.class, ConfigDef.Type.LIST);
    javaClassToKafkaType.put(Class.class, ConfigDef.Type.CLASS);
    javaClassToKafkaType.put(Password.class, ConfigDef.Type.PASSWORD);
  }

  protected void validateReturnType(Method method) {
    // fail fast if return type is unsupported List type
    if (method.getReturnType().equals(List.class)
        && !hasParameters(method.getGenericReturnType(), String.class)) {
      throw new RuntimeException("Method " + method + " has unsupported return type; For lists, only List<String> is supported.");
    }
  }

  protected List<String> getDependents(Method method) {
    return getAnnotation(method, Dependents.class)
        .map(a -> Arrays.asList(a.value()))
        .orElse(emptyList());
  }

  protected String getDisplayName(Method method) {
    return getAnnotation(method, DisplayName.class)
        .map(DisplayName::value)
        .orElseGet(() -> getDefaultDisplayName(method));
  }

  private String getDefaultDisplayName(Method method) {
    String name = insertSpacesBeforeCapitals(method.getName());
    return Character.toUpperCase(name.charAt(0)) + name.substring(1);
  }

  protected String getGroup(Method method) {
    return getAnnotation(method, Group.class).map(Group::value)
        .orElseGet(() -> getDefaultGroup(method));
  }

  protected String getDefaultGroup(Method method) {
    return insertSpacesBeforeCapitals(
        removeSuffix(method.getDeclaringClass().getSimpleName(), "Config"));
  }

  protected List<String> since(MethodJavadoc methodJavadoc) {
    List<String> result = new ArrayList<>();
    for (OtherJavadoc other : methodJavadoc.getOther()) {
      if ("since".equals(other.getName())) {
        result.add(other.getComment().toString());
      }
    }
    return result;
  }

  protected Optional<String> deprecated(MethodJavadoc methodJavadoc) {
    for (OtherJavadoc other : methodJavadoc.getOther()) {
      if ("deprecated".equals(other.getName())) {
        return Optional.of(other.getComment().toString());
      }
    }
    return Optional.empty();
  }

  @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
  protected String getDocumentation(Method method) {
    StringBuilder javadoc = new StringBuilder();

    MethodJavadoc methodJavadoc = RuntimeJavadoc.getJavadoc(method);
    javadoc.append(methodJavadoc.getComment().toString());

    getEnvironmentVariableName(method).ifPresent(envar ->
        javadoc.append("<p>May be overridden with the " + envar + " environment variable."));

    appendContextDocumentation(javadoc, method);

    Stability.Uncommitted uncommitted = method.getAnnotation(Stability.Uncommitted.class);
    if (uncommitted != null) {
      javadoc.append("<p>UNCOMMITTED; this feature may change in a patch release without notice.");
    }

    deprecated(methodJavadoc).ifPresent(message ->
        javadoc.append("<p>WARNING: *DEPRECATED.* " + message));

    List<String> since = since(methodJavadoc);
    if (!since.isEmpty()) {
      javadoc.append("<p>* Since: " + String.join(", ", since));
    }

    return htmlToPlaintext(javadoc.toString());
  }

  protected void appendContextDocumentation(StringBuilder javadoc, Method method) {
    boolean isContextual = method.getReturnType().equals(Contextual.class);
    ContextDocumentation contextDoc = getAnnotation(method, ContextDocumentation.class).orElse(null);

    if (isContextual && contextDoc == null) {
      throw new RuntimeException("Method returning " + Contextual.class.getSimpleName() + " not annotated with with @" + ContextDocumentation.class.getSimpleName() + " ; method = " + method);
    }
    if (!isContextual && contextDoc != null) {
      throw new RuntimeException("Method annotated with @" + ContextDocumentation.class.getSimpleName() + " does not return " + Contextual.class.getSimpleName() + " ; method = " + method);
    }

    if (contextDoc != null) {
      String exampleConfigKey = getConfigKeyName(method) + "[" + contextDoc.sampleContext() + "]=" + contextDoc.sampleValue();
      javadoc.append(
          "<p>*This property supports contextual overrides.* The context is " + contextDoc.contextDescription() + "." +
              " Specify a context by enclosing it in square brackets and appending it to the property name." +
              " When there is no matching override, the value of the base property (without brackets) is used instead." +
              "<p>* Override example: `" + exampleConfigKey + "`"
      );
    }
  }

  protected Optional<String> getEnvironmentVariableName(Method method) {
    return getAnnotation(method, EnvironmentVariable.class)
        .map(EnvironmentVariable::value);
  }

  protected int getOrderInGroup(Method method) {
    // The Reflection API doesn't tell you the order methods are declared in.
    //
    // If the methods have Javadoc and were compiled using the
    // "therapi-runtime-javadoc-scribe" annotation processor,
    // the order of the Javadoc *does* match declaration order.
    ClassJavadoc doc = RuntimeJavadoc.getJavadoc(method.getDeclaringClass());
    int i = 0;
    for (MethodJavadoc methodJavadoc : doc.getMethods()) {
      i++;
      if (methodJavadoc.matches(method)) {
        return i;
      }
    }
    return -1;
  }

  private static Object invokeCompanion(Method method, String suffix) {
    try {
      Method companion = method.getDeclaringClass()
          .getDeclaredMethod(method.getName() + suffix);

      if (!Modifier.isStatic(companion.getModifiers())) {
        throw new RuntimeException("Companion method " + method.getName() + suffix + "() must be static.");
      }
      return companion.invoke(null);

    } catch (NoSuchMethodException e) {
      return null;

    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Failed to invoke " + suffix + " companion method for " + method, e);
    }
  }

  protected ConfigDef.Recommender getRecommender(Method method) {
    ConfigDef.Recommender userProvided = (ConfigDef.Recommender) invokeCompanion(method, "Recommender");
    if (userProvided != null) {
      return userProvided;
    }
    return getDefaultRecommender(method);
  }

  protected ConfigDef.Recommender getDefaultRecommender(Method method) {
    CustomTypeHandler<?> customTypeHandler = customTypeMap.get(method.getReturnType());
    if (customTypeHandler != null) {
      ConfigDef.Recommender v = customTypeHandler.recommender();
      if (v != null) {
        return v;
      }
    }

    if (method.getReturnType().isEnum()) {
      //noinspection unchecked
      return new EnumRecommender((Class<? extends Enum<?>>) method.getReturnType());
    }

    return null;
  }

  protected ConfigDef.Validator getValidator(Method method) {
    ConfigDef.Validator userProvided = (ConfigDef.Validator) invokeCompanion(method, "Validator");
    if (userProvided != null) {
      return userProvided;
    }

    return getDefaultValidator(method);
  }

  private static @Nullable Type contexualValueType(Type maybeContextual) {
    if (!(maybeContextual instanceof ParameterizedType)) {
      return null;
    }

    ParameterizedType parameterizedType = (ParameterizedType) maybeContextual;
    if (parameterizedType.getRawType() != Contextual.class) {
      return null;
    }

    return parameterizedType.getActualTypeArguments()[0];
  }

  private static @Nullable Class<?> contextualValueClass(Type maybeContextualType) {
    Type valueType = contexualValueType(maybeContextualType);
    if (valueType == null) {
      return null;
    }
    if (isListOfStrings(valueType)) {
      return List.class;
    }
    if (valueType instanceof Class) {
      return (Class<?>) valueType;
    }

    throw new IllegalArgumentException(valueType + " is not a supported Contextual value type; must be a raw class or List<String>");
  }

  protected ConfigDef.Validator getDefaultValidator(Method method) {
    Class<?> type = contextualValueClass(method.getGenericReturnType());
    if (type == null) {
      type = method.getReturnType();
    }

    CustomTypeHandler<?> customTypeHandler = customTypeMap.get(type);
    if (customTypeHandler != null) {
      ConfigDef.Validator v = customTypeHandler.validator();
      if (v != null) {
        return v;
      }
    }

    if (method.getReturnType().isEnum()) {
      //noinspection unchecked
      return new EnumValidator((Class<? extends Enum<?>>) method.getReturnType());
    }

    return null;
  }

  protected Object getDefaultValue(Method method) {
    return getAnnotation(method, Default.class)
        .map(a -> (Object) a.value())
        .orElse(ConfigDef.NO_DEFAULT_VALUE);
  }

  protected ConfigDef.Type getKafkaType(Method method) {
    Class<?> returnType = method.getReturnType();
    ConfigDef.Type kafkaType = getKafkaType(returnType);
    if (kafkaType != null) {
      return kafkaType;
    }

    Class<?> contextualValueClass = contextualValueClass(method.getGenericReturnType());
    if (contextualValueClass != null) {
      kafkaType = getKafkaType(contextualValueClass);
    }

    if (kafkaType == null) {
      throw new RuntimeException("Method " + method + " has unsupported return type: " + method.getGenericReturnType());
    }

    return kafkaType;
  }

  protected ConfigDef.@Nullable Type getKafkaType(Class<?> type) {
    ConfigDef.Type kafkaType = javaClassToKafkaType.get(type);
    if (kafkaType != null) {
      return kafkaType;
    }

    if (type.isEnum()) {
      return ConfigDef.Type.STRING;
    }

    return null;
  }

  /**
   * Exposes the {@link AbstractConfig#get(String)} method so the dynamic proxy
   * doesn't need to call the type-specific methods (like getString, getBoolean, etc).
   */
  public static class ConcreteKafkaConfig extends AbstractConfig {
    public ConcreteKafkaConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
      super(definition, originals, doLog);
    }

    public Object get(String key) {
      return super.get(key);
    }
  }

  protected static <T extends Annotation> Optional<T> getAnnotation(Method method, Class<T> annotationClass) {
    T annotation = method.getAnnotation(annotationClass);
    if (annotation != null) {
      return Optional.of(annotation);
    }
    return Optional.ofNullable(
        method.getDeclaringClass()
            .getAnnotation(annotationClass));
  }

  protected String getConfigKeyName(Method method) {
    return prefix + lowerCamelCaseToDottedLowerCase(method.getName());
  }

  protected static String lowerCamelCaseToDottedLowerCase(String name) {
    return name.replaceAll("(\\p{javaUpperCase})", ".$1")
        .toLowerCase(Locale.ROOT);
  }

  protected ConfigDef.Width getWidth(Method method) {
    return getAnnotation(method, Width.class)
        .map(Width::value)
        .orElse(ConfigDef.Width.NONE);
  }

  protected ConfigDef.Importance getImportance(Method method) {
    return getAnnotation(method, Importance.class)
        .map(Importance::value)
        .orElse(ConfigDef.Importance.MEDIUM);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected Enum<?> parseEnum(Class<?> enumClass, String value) {
    return Enum.valueOf((Class) enumClass, value);
  }

  protected static String insertSpacesBeforeCapitals(String s) {
    return s.replaceAll("(\\p{javaUpperCase})", " $1").trim();
  }

  protected static String removeSuffix(String s, String suffix) {
    if (s.endsWith(suffix)) {
      s = s.substring(0, s.length() - suffix.length());
    }
    return s;
  }

  protected static boolean hasParameters(Type t, Type... paramTypes) {
    if (!(t instanceof ParameterizedType)) {
      return false;
    }
    return Arrays.equals(((ParameterizedType) t).getActualTypeArguments(), paramTypes);
  }
}
