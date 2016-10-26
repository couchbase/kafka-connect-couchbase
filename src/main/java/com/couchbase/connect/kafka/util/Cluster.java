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

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.deps.io.netty.handler.codec.base64.Base64;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class Cluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    public static Config fetchBucketConfig(String bucket, String password, List<String> nodes) {
        final AtomicReference<CouchbaseBucketConfig> result = new AtomicReference<CouchbaseBucketConfig>(null);
        NioEventLoopGroup group = new NioEventLoopGroup();
        try {
            for (final String hostname : nodes) {
                try {
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
                                                            String body = msg.content().toString(CharsetUtil.UTF_8).replace("$HOST", hostname);
                                                            result.set((CouchbaseBucketConfig) BucketConfigParser.parse(body));
                                                        }
                                                    } finally {
                                                        latch.countDown();
                                                    }
                                                }
                                            });
                                }
                            });


                    Channel channel = bootstrap.connect(hostname, 8091).sync().channel();
                    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                            "/pools/default/b/" + bucket);
                    request.headers().set(HttpHeaders.Names.HOST, hostname);
                    request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);

                    ByteBuf raw = Unpooled.buffer(bucket.length() + password.length() + 1);
                    raw.writeBytes((bucket + ":" + password).getBytes(CharsetUtil.UTF_8));
                    ByteBuf encoded = Base64.encode(raw, false);
                    request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CharsetUtil.UTF_8));
                    encoded.release();
                    raw.release();

                    channel.writeAndFlush(request);
                    latch.await();
                    channel.closeFuture().sync();
                    CouchbaseBucketConfig config = result.get();
                    if (config != null) {
                        return new Config(config);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Ignoring error for node {} when getting number of partitions", hostname, e);
                }
            }
        } finally {
            group.shutdownGracefully();
        }
        return null;
    }
}
