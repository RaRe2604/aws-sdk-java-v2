/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.benchmark;

import static java.util.Collections.singletonMap;

import io.netty.channel.nio.NioEventLoopGroup;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public class NettyImprovementsBenchmark {
    private static final String TABLE = "NettyImprovementsBenchmark";
    private static final String KEY_ATTRIBUTE_NAME = "id";
    private static final AttributeValue KEY_ATTRIBUTE_VALUE = AttributeValue.fromS("foo");
    private static final Duration WARMUP_DURATION = Duration.ofMinutes(1);
    private static final Duration TEST_DURATION = Duration.ofMinutes(5);
    private static final int MAX_CONCURRENCY = 500;

    private static final DynamoDbClient syncClient;

    static {
        syncClient = DynamoDbClient.builder().httpClientBuilder(ApacheHttpClient.builder()).build();
        try {
            syncClient.describeTable(r -> r.tableName(TABLE));
        } catch (ResourceNotFoundException e) {
            syncClient.createTable(r -> r.tableName(TABLE)
                                         .billingMode(BillingMode.PAY_PER_REQUEST)
                                         .attributeDefinitions(a -> a.attributeName(KEY_ATTRIBUTE_NAME)
                                                                     .attributeType(ScalarAttributeType.S))
                                         .keySchema(k -> k.attributeName(KEY_ATTRIBUTE_NAME)
                                                          .keyType(KeyType.HASH)));
            syncClient.waiter().waitUntilTableExists(r -> r.tableName(TABLE));
            syncClient.putItem(r -> r.tableName(TABLE).item(singletonMap(KEY_ATTRIBUTE_NAME, KEY_ATTRIBUTE_VALUE)));
        }

    }

    public static void main(String... args) throws Exception {
        sameEventLoopCaller();
    }

    public static void separateEventLoopCaller() throws Exception {
        NioEventLoopGroup newEventLoopGroup = new NioEventLoopGroup();
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        eventLoopTest(newEventLoopGroup, eventLoopGroup, "separateEventLoopCaller");
        eventLoopGroup.shutdownGracefully();
        newEventLoopGroup.shutdownGracefully();
    }

    public static void sameEventLoopCaller() throws Exception {
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        eventLoopTest(eventLoopGroup, eventLoopGroup, "sameEventLoopCaller");
        eventLoopGroup.shutdownGracefully();
    }

    private static void eventLoopTest(NioEventLoopGroup callerGroup, NioEventLoopGroup nettyGroup, String test) throws InterruptedException {
        SdkEventLoopGroup separateEventLoop = SdkEventLoopGroup.create(nettyGroup);
        NettyNioAsyncHttpClient.Builder separateEventLoopNetty =
            NettyNioAsyncHttpClient.builder()
                                   .eventLoopGroup(separateEventLoop)
                                   .maxConcurrency(MAX_CONCURRENCY);
        DynamoDbAsyncClient ddb = DynamoDbAsyncClient.builder()
                                                     .httpClientBuilder(separateEventLoopNetty)
                                                     .asyncConfiguration(c -> c.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, Runnable::run))
                                                     .build();

        Semaphore requestLimiter = new Semaphore(MAX_CONCURRENCY);

        Instant warmStart = Instant.now();
        Instant warmEnd = warmStart.plus(WARMUP_DURATION);

        System.out.println("Warming " + test + " until " + warmEnd);
        while (Instant.now().isBefore(warmEnd)) {
            requestLimiter.acquire();
            callerGroup.execute(() -> {
                try {
                    getItem(ddb).handle((r, t) -> {
                        requestLimiter.release();
                        return null;
                    });
                } catch (Throwable t) {
                    requestLimiter.release();
                }
            });
        }

        requestLimiter.acquire(MAX_CONCURRENCY);
        requestLimiter.release(MAX_CONCURRENCY);

        Instant start = Instant.now();
        Instant end = start.plus(TEST_DURATION);
        Instant lastLog = Instant.now();
        AtomicLong requestsSuccess = new AtomicLong(0);
        AtomicLong requestsFailed = new AtomicLong(0);

        System.out.println("Running " + test + " until " + end);
        while (Instant.now().isBefore(end)) {
            if (Duration.between(lastLog, Instant.now()).getSeconds() >= 10) {
                Duration currentDuration = Duration.between(start, Instant.now());
                System.out.println("TPS: " + (requestsSuccess.get() * 1.0 / currentDuration.getSeconds()) +
                                   ", Avg Latency: " + (currentDuration.toMillis() * 1.0 / requestsSuccess.get()) + " ms" +
                                   ", Failures: " + requestsFailed.get());
                lastLog = Instant.now();
            }

            requestLimiter.acquire();
            callerGroup.execute(() -> {
                try {
                    getItem(ddb).handle((r, t) -> {
                        requestLimiter.release();
                        if (t != null) {
                            requestsFailed.incrementAndGet();
                        } else {
                            requestsSuccess.incrementAndGet();
                        }
                        return null;
                    });
                } catch (Exception e) {
                    requestsFailed.incrementAndGet();
                    requestLimiter.release();
                } catch (Throwable t) {
                    t.printStackTrace();
                    requestsFailed.incrementAndGet();
                    requestLimiter.release();
                }
            });
        }
    }

    public static void separateThreadPoolCaller() throws Exception {
        NioEventLoopGroup vanillaEventLoopGroup = new NioEventLoopGroup();
        SdkEventLoopGroup vanillaEventLoop = SdkEventLoopGroup.create(vanillaEventLoopGroup);
        NettyNioAsyncHttpClient.Builder vanillaEventLoopNetty =
            NettyNioAsyncHttpClient.builder()
                                   .eventLoopGroup(vanillaEventLoop)
                                   .maxConcurrency(MAX_CONCURRENCY);
        DynamoDbAsyncClient ddb = DynamoDbAsyncClient.builder()
                                                     .httpClientBuilder(vanillaEventLoopNetty)
                                                     .asyncConfiguration(c -> c.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, Runnable::run))
                                                     .build();

        ExecutorService executor = Executors.newFixedThreadPool(MAX_CONCURRENCY);

        Instant warmStart = Instant.now();
        Instant warmEnd = warmStart.plus(WARMUP_DURATION);

        System.out.println("Warming separateThreadPoolCaller until " + warmEnd);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < MAX_CONCURRENCY; i++) {
            futures.add(executor.submit(() -> {
                while (Instant.now().isBefore(warmEnd)) {
                    getItem(ddb).join();
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        Instant start = Instant.now();
        Instant end = start.plus(TEST_DURATION);
        AtomicLong requestsSuccess = new AtomicLong(0);
        AtomicLong requestsFailed = new AtomicLong(0);

        System.out.println("Running separateThreadPoolCaller until " + end);
        for (int i = 0; i < MAX_CONCURRENCY; i++) {
            executor.submit(() -> {
                while (Instant.now().isBefore(end)) {
                    try {
                        getItem(ddb).join();
                        requestsSuccess.incrementAndGet();
                    } catch (Exception e) {
                        requestsFailed.incrementAndGet();
                    } catch (Throwable t) {
                        t.printStackTrace();
                        requestsFailed.incrementAndGet();
                    }
                }
            });
        }

        while (Instant.now().isBefore(end)) {
            Thread.sleep(10_000);
            Duration currentDuration = Duration.between(start, Instant.now());
            System.out.println("TPS: " + (requestsSuccess.get() * 1.0 / currentDuration.getSeconds()) +
                               ", Avg Latency: " + (currentDuration.toMillis() * 1.0 / requestsSuccess.get()) + " ms" +
                               ", Failures: " + requestsFailed.get());
        }

    }

    private static CompletableFuture<GetItemResponse> getItem(DynamoDbAsyncClient ddb) {
        return ddb.getItem(r -> r.tableName(TABLE).key(singletonMap(KEY_ATTRIBUTE_NAME, KEY_ATTRIBUTE_VALUE)));
    }

}
