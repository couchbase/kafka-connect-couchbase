/*
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
import com.couchbase.client.core.env.ConfigParserEnvironment;
import com.couchbase.client.core.node.DefaultMemcachedHashingStrategy;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.utils.NetworkAddress;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.SSLEngineFactory;
import com.couchbase.client.dcp.config.SecureEnvironment;
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
import com.couchbase.client.deps.io.netty.handler.ssl.SslHandler;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.connect.kafka.CouchbaseSourceConnectorConfig;
import com.couchbase.connect.kafka.util.config.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.logging.RedactableArgument.system;

public class Cluster {
    static final ConfigParserEnvironment dummyBootstrapEnv = new ConfigParserEnvironment() {
        @Override
        public MemcachedHashingStrategy memcachedHashingStrategy() {
            return DefaultMemcachedHashingStrategy.INSTANCE;
        }
    };
    private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

    public static Config fetchBucketConfig(final CouchbaseSourceConnectorConfig config) {
        final List<String> nodes = config.getList(CouchbaseSourceConnectorConfig.CONNECTION_CLUSTER_ADDRESS_CONFIG);
        final String bucket = config.getString(CouchbaseSourceConnectorConfig.CONNECTION_BUCKET_CONFIG);
        final String username = config.getUsername();
        final String password = Password.CONNECTION.get(config);
        final boolean sslEnabled = config.getBoolean(CouchbaseSourceConnectorConfig.CONNECTION_SSL_ENABLED_CONFIG);
        final int port = sslEnabled ? ClientEnvironment.BOOTSTRAP_HTTP_SSL_PORT : ClientEnvironment.BOOTSTRAP_HTTP_DIRECT_PORT;
        final SSLEngineFactory sslEngineFactory =
                new SSLEngineFactory(new SecureEnvironment() {
                    @Override
                    public boolean sslEnabled() {
                        return sslEnabled;
                    }

                    @Override
                    public String sslKeystoreFile() {
                        return config.getString(CouchbaseSourceConnectorConfig.CONNECTION_SSL_KEYSTORE_LOCATION_CONFIG);
                    }

                    @Override
                    public String sslKeystorePassword() {
                        return Password.SSL_KEYSTORE.get(config);
                    }

                    @Override
                    public KeyStore sslKeystore() {
                        return null;
                    }
                });

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
                                    if (sslEnabled) {
                                        pipeline.addLast(new SslHandler(sslEngineFactory.get()));
                                    }

                                    pipeline.addLast(new HttpClientCodec())
                                            .addLast(new HttpObjectAggregator(1048576))
                                            .addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
                                                @Override
                                                protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
                                                    try {
                                                        if (msg.getStatus().equals(HttpResponseStatus.OK)) {
                                                            String body = msg.content().toString(CharsetUtil.UTF_8).replace("$HOST", hostname);
                                                            result.set((CouchbaseBucketConfig) BucketConfigParser.parse(body, dummyBootstrapEnv, NetworkAddress.create(hostname)));
                                                        }
                                                    } finally {
                                                        latch.countDown();
                                                    }
                                                }
                                            });
                                }
                            });


                    Channel channel = bootstrap.connect(hostname, port).sync().channel();
                    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                            "/pools/default/b/" + bucket);
                    request.headers().set(HttpHeaders.Names.HOST, hostname);
                    request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);

                    ByteBuf raw = Unpooled.buffer(bucket.length() + password.length() + 1);
                    raw.writeBytes((username + ":" + password).getBytes(CharsetUtil.UTF_8));
                    ByteBuf encoded = Base64.encode(raw, false);
                    request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CharsetUtil.UTF_8));
                    encoded.release();
                    raw.release();

                    channel.writeAndFlush(request);
                    latch.await();
                    channel.closeFuture().sync();
                    CouchbaseBucketConfig bucketConfig = result.get();
                    if (bucketConfig != null) {
                        return new Config(bucketConfig);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Ignoring error for node {} when getting number of partitions", system(hostname), e);
                }
            }
        } finally {
            group.shutdownGracefully();
        }
        return null;
    }
}
