/**
 * Copyright 2016 Couchbase, Inc.
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

package com.couchbase.connect.kafka.util;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpClientCodec;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpHeaders;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpObjectAggregator;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpVersion;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class Schemas {
    public static final Schema KEY_SCHEMA = SchemaBuilder.string().build();
    public static final Schema VALUE_DEFAULT_SCHEMA =
            SchemaBuilder.struct().name("com.couchbase.DcpMessage")
                    .field("event", Schema.STRING_SCHEMA)
                    .field("partition", Schema.INT16_SCHEMA)
                    .field("key", Schema.STRING_SCHEMA)
                    .field("cas", Schema.INT64_SCHEMA)
                    .field("bySeqno", Schema.INT64_SCHEMA)
                    .field("revSeqno", Schema.INT64_SCHEMA)
                    .field("expiration", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("flags", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("lockTime", Schema.OPTIONAL_INT32_SCHEMA)
                    .field("content", Schema.OPTIONAL_BYTES_SCHEMA)
                    .build();
    private static final Logger LOGGER = LoggerFactory.getLogger(Schemas.class);
    private static ObjectMapper JSON = new ObjectMapper();

    public static String fetchSchemaString(String uri) {
        final AtomicReference<String> result = new AtomicReference<String>(null);
        NioEventLoopGroup group = new NioEventLoopGroup();

        try {
            final URI schemaUri = new URI(uri);
            final CountDownLatch latch = new CountDownLatch(1);
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            pipeline.addLast(new HttpClientCodec())
                                    .addLast(new HttpObjectAggregator(1048576))
                                    .addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
                                            try {
                                                if (msg.getStatus().equals(HttpResponseStatus.OK)) {
                                                    JsonNode body = JSON.readTree(msg.content().toString(CharsetUtil.UTF_8));
                                                    result.set(body.get("schema").asText());
                                                }
                                            } finally {
                                                latch.countDown();
                                            }
                                        }
                                    });
                        }
                    });


            Channel channel = bootstrap.connect(schemaUri.getHost(), schemaUri.getPort()).sync().channel();
            HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, schemaUri.getPath());
            request.headers().set(HttpHeaders.Names.HOST, schemaUri.getHost());
            request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
            request.headers().set(HttpHeaders.Names.CONTENT_TYPE, Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
            channel.writeAndFlush(request);
            latch.await();
            channel.closeFuture().sync();
            return result.get();
        } catch (URISyntaxException e) {
            LOGGER.warn("Failed to parse {} as URI", uri, e);
        } catch (InterruptedException e) {
            LOGGER.warn("Failed to execute HTTP request", e);
        } finally {
            group.shutdownGracefully();
        }
        return null;
    }

}
