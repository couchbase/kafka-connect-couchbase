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

import org.apache.kafka.common.config.ConfigDef;

/**
 * This utility class provides common functionality that can be reused for
 * multiple Config Providers.
 *
 */
public class CommonConfigUtils {

    /** Common configuration parameters to be reused in multiple Config Providers */
    public static final ConfigDef COMMON_CONFIG = getConfig();

    public static final String REGION = "region";
    private static final String REGION_DOC = "Specify region if needed. Default region is the same where connector is running";

    public static final String ENDPOINT = "endpoint";
    private static final String ENDPOINT_DOC = "(Optional) Specify an endpoint. Default endpoint will be used. "
            + "If there is an endpoint for a service in a VPC, and it should be used instead of default one, "
            + "this is the way to explicitly provide one.";

    /**
     * 
     * @return common configuration
     */
    static ConfigDef getConfig() {
        return new ConfigDef()
                .define(
                        REGION,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.MEDIUM,
                        REGION_DOC)
                .define(
                        ENDPOINT,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.MEDIUM,
                        ENDPOINT_DOC);
    }
}
