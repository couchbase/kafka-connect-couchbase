
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
package com.amazonaws.kafka.config.providers.common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigChangeCallback;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;

public abstract class AwsServiceConfigProvider implements ConfigProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private String region;
    private String endpoint;

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Set common AWS configuration parameters, like region, endpoint, etc...
     * 
     * @param config
     */
    public void setCommonConfig(AbstractConfig config) {

        // default region from configuration. It can be null, empty or blank.
        this.region = config.getString(CommonConfigUtils.REGION);
        this.endpoint = config.getString(CommonConfigUtils.ENDPOINT);
    }

    @Override
    public void subscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
        log.info("Subscription is not implemented and will be ignored");
    }

    public void close() throws IOException {
    }

    /**
     * Set configuration that is common to most AWS Clients.
     * 
     * @param <T>
     * @param cBuilder
     * @return
     */
    protected <T extends AwsClientBuilder<?, ?>> T setClientCommonConfig(T cBuilder) {

        if (this.region != null && !this.region.isBlank()) {
            cBuilder.region(Region.of(this.region));
        }

        if (this.endpoint != null && !endpoint.isBlank())
            try {
                cBuilder.endpointOverride(new URI(this.endpoint));
            } catch (URISyntaxException e) {
                log.error("Invalid syntax, ", e);
                throw new RuntimeException(e);
            }

        return cBuilder;
    }

    protected String parseKey(String keyWithOptions) {
        if (keyWithOptions == null)
            return keyWithOptions;

        return keyWithOptions.split("\\?", 2)[0];
    }

    protected Map<String, String> parseKeyOptions(String keyWithOptions) {

        Map<String, String> options = new LinkedHashMap<>();

        if (keyWithOptions == null)
            return options;

        String[] parsed = keyWithOptions.split("\\?", 2);
        if (parsed.length < 2)
            return options;

        Matcher m = Pattern.compile("(\\w+)=(.*?)(?=,\\w+=|$)").matcher(parsed[1]);
        while (m.find()) {
            options.put(m.group(1), m.group(2));
        }

        return options;
    }

    protected Long getUpdatedTtl(Long currentTtl, Map<String, String> options) {
        if (options == null || options.isEmpty())
            return currentTtl;

        String newTtlStr = options.get("ttl");
        if (newTtlStr == null)
            return currentTtl;

        try {
            Long newTtl = Long.valueOf(newTtlStr);
            return currentTtl == null || currentTtl > newTtl
                    ? newTtl
                    : currentTtl;
        } catch (Exception e) {
            log.warn("TTL value '{}' is not a number", newTtlStr, e);
        }
        return currentTtl;
    }
}
