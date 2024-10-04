/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.amazonaws.kafka.config.providers;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigChangeCallback;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.kafka.config.providers.common.AwsServiceConfigProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

/**
 * This class implements a ConfigProvider for AWS Secrets Manager.<br>
 *
 * <p>
 * <b>Usage:</b><br>
 * In a configuration file (e.g. {@code client.properties}) define following
 * properties:<br>
 *
 * <pre>
 * #        Step1. Configure the secrets manager as config provider:
 * config.providers=secretsmanager
 * config.providers.secretsmanager.class=com.amazonaws.kafka.config.providers.SecretsMamagerConfigProvider
 * # optional parameter for region:
 * config.providers.secretsmanager.param.region=us-west-2
 * # optional parameter, see more details below
 * config.providers.secretsmanager.param.NotFoundStrategy=fail
 *
 * #        Step 2. Usage of AWS secrets manager as config provider:
 * db.username=${secretsmanager:AmazonMSK_TestKafkaConfig:username}
 * db.password=${secretsmanager:AmazonMSK_TestKafkaConfig:password}
 * </pre>
 *
 * Note, this config provider implementation assumes secret values will be
 * returned in Json format.
 * Nested values aren't supported at this point.<br>
 *
 * SecretsManagerConfigProvider can be configured using parameters.<br>
 * Format:<br>
 * {@code config.providers.secretsmanager.param.<param_name> = <param_value>}<br>
 *
 * @param region           - defines a region to get a secret from.
 * @param NotFoundStrategy - defines an action in case requested secret or a key
 *                         in a value cannot be resolved. <br>
 *                         <ul>
 *                         Passible values are:
 *                         <ul>
 *                         {@code fail} - (Default) the code will throw an
 *                         exception {@code ConfigNotFoundException}
 *                         </ul>
 *                         <ul>
 *                         {@code ignore} - a value will remain with tokens
 *                         without any change
 *                         </ul>
 *                         </ul>
 *
 *
 *                         Expression usage:<br>
 *                         <code>property_name=${secretsmanager:secret.id:secret.key}</code>
 *
 */
public class SecretsManagerConfigProvider extends AwsServiceConfigProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final String EMPTY = "";

    private SecretsManagerConfig config;
    private String notFoundStrategy;

    private SecretsManagerClientBuilder cBuilder;

    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new SecretsManagerConfig(configs);
        setCommonConfig(config);

        this.notFoundStrategy = config.getString(SecretsManagerConfig.NOT_FOUND_STRATEGY);

        // set up a builder:
        this.cBuilder = SecretsManagerClient.builder();
        setClientCommonConfig(this.cBuilder);
    }

    /**
     * Retrieves a secret from AWS Secrets Manager
     *
     * @param path the path in Parameters Store
     * @return the configuration data
     */
    @Override
    public ConfigData get(String path) {
        return get(path, Collections.emptySet());
    }

    /**
     * Retrieves secret's fields from a given secret.
     *
     * @param encodedPath AWS Secrets Manager
     *                    <code> secret name or encoded ARN </code>
     * @param keys        fields inside a given secret.
     * @return the configuration data
     */
    @Override
    public ConfigData get(String encodedPath, Set<String> keys) {
        Map<String, String> data = new HashMap<>();
        if (encodedPath == null || encodedPath.isEmpty()
                || keys == null || keys.isEmpty()) {
            // if no fields provided, just ignore this usage
            return new ConfigData(data);
        }

        String path = URLDecoder.decode(encodedPath, StandardCharsets.UTF_8);
        GetSecretValueRequest request = GetSecretValueRequest.builder().secretId(path).build();
        Map<String, String> secretJson = null;
        try {
            SecretsManagerClient secretsClient = checkOrInitSecretManagerClient();
            GetSecretValueResponse response = secretsClient.getSecretValue(request);
            String value = response.secretString();

            try {
                secretJson = new ObjectMapper().readValue(value, new TypeReference<>() {
                });
            } catch (Exception e) {
                log.error("Unexpected value of a secret's structure", e);
                throw new ConfigException(path, value, "Unexpected value of a secret's structure");
            }
        } catch (ResourceNotFoundException e) {
            log.info(
                    "Secret id {} not found. Value will be handled according to a strategy defined by 'NotFoundStrategy'",
                    path);
            handleNotFoundByStrategy(data, path, null, e);
        }

        Long ttl = null;
        for (String keyWithOptions : keys) {
            String key = parseKey(keyWithOptions);
            Map<String, String> options = parseKeyOptions(keyWithOptions);
            ttl = getUpdatedTtl(ttl, options);

            // secretJson can be null at this point only if there is a permissive strategy.
            if (secretJson == null) {
                data.put(key, EMPTY);
                continue;
            }
            if (secretJson.containsKey(key)) {
                data.put(keyWithOptions, secretJson.get(key));
            } else {
                log.info("Secret {} doesn't have a key {}.", path, key);
                handleNotFoundByStrategy(data, path, key, null);
            }
        }

        return ttl == null ? new ConfigData(data) : new ConfigData(data, ttl);
    }

    protected SecretsManagerClient checkOrInitSecretManagerClient() {
        return cBuilder.build();
    }

    @Override
    public void subscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
        log.info("Subscription is not implemented and will be ignored");
    }

    @Override
    public void close() throws IOException {
    }

    private void handleNotFoundByStrategy(Map<String, String> data, String path, String key, RuntimeException e) {
        if (SecretsManagerConfig.NOT_FOUND_IGNORE.equals(this.notFoundStrategy)
                && key != null && !key.isBlank()) {
            data.put(key, "");
        } else if (SecretsManagerConfig.NOT_FOUND_FAIL.equals(this.notFoundStrategy)) {
            if (e != null) {
                throw e;
            } else {
                throw new ConfigException(String.format("Secret undefined {}:{}", path, key));
            }
        }
    }
}
