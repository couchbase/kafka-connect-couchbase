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

package com.couchbase.connect.kafka;

import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.DisconnectRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class CouchbaseMonitorThread extends Thread {
    private static final Random RND = new Random();
    private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseMonitorThread.class);

    private final String clusterAddress;
    private final String bucket;
    private final Password password;
    private final BlockingQueue<DCPRequest> queue;
    private final String connectionName;
    private final CouchbaseCore core;
    private final long connectionTimeout;

    public CouchbaseMonitorThread(String clusterAddress, String bucket, Password password, long connectionTimeout,
                                  BlockingQueue<DCPRequest> queue) {
        this.clusterAddress = clusterAddress;
        this.bucket = bucket;
        this.password = password;
        this.connectionTimeout = connectionTimeout;
        this.queue = queue;
        this.connectionName = generateConnectionName();
        CoreEnvironment env = DefaultCoreEnvironment.builder()
                .dcpEnabled(true)
                .dcpConnectionName(connectionName)
                .build();

        core = new CouchbaseCore(env);
    }

    private static String generateConnectionName() {
        return "cbConnect-" + System.currentTimeMillis() + "-" + RND.nextInt();
    }

    @Override
    public void run() {
        OpenConnectionResponse response = core
                .<SeedNodesResponse>send(new SeedNodesRequest(clusterAddress))
                .flatMap(new Func1<SeedNodesResponse, Observable<OpenBucketResponse>>() {
                    @Override
                    public Observable<OpenBucketResponse> call(SeedNodesResponse response) {
                        return core.send(new OpenBucketRequest(bucket, password.value()));
                    }
                })
                .flatMap(new Func1<OpenBucketResponse, Observable<OpenConnectionResponse>>() {
                    @Override
                    public Observable<OpenConnectionResponse> call(OpenBucketResponse response) {
                        return core.send(new OpenConnectionRequest(connectionName, bucket));
                    }
                })
                .timeout(connectionTimeout, TimeUnit.MILLISECONDS)
                .toBlocking()
                .single();
        LOGGER.info("Established DCP connection {}", connectionName);
        final DCPConnection connection = response.connection();
        Observable.range(0, numberOfPartitions())
                .flatMap(new Func1<Integer, Observable<ResponseStatus>>() {
                    @Override
                    public Observable<ResponseStatus> call(Integer partition) {
                        return connection.addStream(partition.shortValue());
                    }
                })
                .toList()
                .flatMap(new Func1<List<ResponseStatus>, Observable<DCPRequest>>() {
                    @Override
                    public Observable<DCPRequest> call(List<ResponseStatus> statuses) {
                        return connection.subject();
                    }
                })
                .toBlocking()
                .forEach(new Action1<DCPRequest>() {
                    @Override
                    public void call(DCPRequest event) {
                        try {
                            queue.put(event);
                        } catch (InterruptedException e) {
                            LOGGER.error("Unable to put DCP request into the queue", e);
                        }
                    }
                });
    }

    public void shutdown() {
        LOGGER.info("Shutting down DCP connection {}", connectionName);
        core.send(new DisconnectRequest()).subscribe();
    }

    private int numberOfPartitions() {
        return core
                .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .map(new Func1<GetClusterConfigResponse, Integer>() {
                    @Override
                    public Integer call(GetClusterConfigResponse response) {
                        CouchbaseBucketConfig config = (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                        return config.numberOfPartitions();
                    }
                }).toBlocking().singleOrDefault(0);
    }
}
