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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.Charsets;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

public class SecretsManagerConfigProviderTest {

    Map<String, Object> props;
    @BeforeEach
    public void setup() {
        props = new HashMap<>();
        props.put("config.providers", "secretsmanager");
        props.put("config.providers.secretsmanager.class", "com.amazonaws.kafka.config.providers.MockedSecretsManagerConfigProvider");
        props.put("config.providers.secretsmanager.param.region", "us-west-2");
        props.put("config.providers.secretsmanager.param.NotFoundStrategy", "fail");
    }
    
    @Test
    public void testExistingKeys() {
        props.put("username", "${secretsmanager:AmazonMSK_TestKafkaConfig:username}");
        props.put("password", "${secretsmanager:AmazonMSK_TestKafkaConfig:password}");

        CustomConfig testConfig = new CustomConfig(props);

        assertEquals("John", testConfig.getString("username"));
        assertEquals("Password123", testConfig.getString("password"));
    }

    @Test
    public void testExistingKeysViaArn() {
        String arn = URLEncoder.encode("arn:aws:secretsmanager:ap-southeast-2:123456789:secret:AmazonMSK_my_service/my_secret", StandardCharsets.UTF_8);
        props.put("username", "${secretsmanager:" + arn + ":username}");
        props.put("password", "${secretsmanager:" + arn + ":password}");

        CustomConfig testConfig = new CustomConfig(props);

        assertEquals("John2", testConfig.getString("username"));
        assertEquals("Password567", testConfig.getString("password"));
    }

    @Test
    public void testExistingKeysViaArnWithEncodedValue() {
        String arn = URLEncoder.encode("arn:aws:secretsmanager:ap-southeast-2:123456789:secret:AmazonMSK_my_service/my_secret%3A", StandardCharsets.UTF_8);
        props.put("username", "${secretsmanager:" + arn + ":username}");
        props.put("password", "${secretsmanager:" + arn + ":password}");

        CustomConfig testConfig = new CustomConfig(props);

        assertEquals("John3", testConfig.getString("username"));
        assertEquals("Password321", testConfig.getString("password"));
    }

    @Test
    public void testExistingKeysViaHandEncodedArn() {
        String arn = "arn%3Aaws%3Asecretsmanager%3Aap-southeast-2%3A123456789%3Asecret%3AAmazonMSK_my_service%2Fmy_secret";
        props.put("username", "${secretsmanager:" + arn + ":username}");
        props.put("password", "${secretsmanager:" + arn + ":password}");

        CustomConfig testConfig = new CustomConfig(props);

        assertEquals("John2", testConfig.getString("username"));
        assertEquals("Password567", testConfig.getString("password"));
    }

    @Test
    public void testTtl() {
        props.put("username", "${secretsmanager:AmazonMSK_TestKafkaConfig:username?ttl=60000}");
        props.put("password", "${secretsmanager:AmazonMSK_TestKafkaConfig:password}");
        
        CustomConfig testConfig = new CustomConfig(props);
        
        assertEquals("John", testConfig.getString("username"));
        assertEquals("Password123", testConfig.getString("password"));
    }

    @Test
    public void testNonExistingSecret() {
        props.put("notFound", "${secretsmanager:notFound:noKey}");
        assertThrows(ResourceNotFoundException.class, () ->new CustomConfig(props));
    }
    
    @Test
    public void testNonExistingKey() {
        props.put("notFound", "${secretsmanager:AmazonMSK_TestKafkaConfig:noKey}");
        assertThrows(ConfigException.class, () ->new CustomConfig(props));
    }
    
    static class CustomConfig extends AbstractConfig {
        final static String DEFAULT_DOC = "Default Doc";
        final static ConfigDef CONFIG = new ConfigDef()
                .define("username", Type.STRING, "defaultValue", Importance.HIGH, DEFAULT_DOC)
                .define("password", Type.STRING, "defaultValue", Importance.HIGH, DEFAULT_DOC)
                ;
        public CustomConfig(Map<?, ?> originals) {
            super(CONFIG, originals);
        }
    }

}
