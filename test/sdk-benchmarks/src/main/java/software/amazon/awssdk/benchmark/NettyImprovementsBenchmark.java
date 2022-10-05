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
        separateThreadPoolCaller();
    }

    public static void separateEventLoopCaller() {
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        SdkEventLoopGroup separateEventLoop = SdkEventLoopGroup.create(eventLoopGroup);
        NettyNioAsyncHttpClient.Builder separateEventLoopNetty =
            NettyNioAsyncHttpClient.builder()
                                   .eventLoopGroup(separateEventLoop)
                                   .maxConcurrency(MAX_CONCURRENCY);
        DynamoDbAsyncClient ddb = DynamoDbAsyncClient.builder()
                                                     .httpClientBuilder(separateEventLoopNetty)
                                                     .asyncConfiguration(c -> c.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, Runnable::run))
                                                     .build();
    }

    public static void sameEventLoopCaller() {
        NioEventLoopGroup sharedEventLoopGroup = new NioEventLoopGroup();
        SdkEventLoopGroup sharedEventLoop = SdkEventLoopGroup.create(sharedEventLoopGroup);
        NettyNioAsyncHttpClient.Builder sharedEventLoopNetty =
            NettyNioAsyncHttpClient.builder()
                                   .eventLoopGroup(sharedEventLoop)
                                   .maxConcurrency(MAX_CONCURRENCY);
        DynamoDbAsyncClient ddb = DynamoDbAsyncClient.builder()
                                                     .httpClientBuilder(sharedEventLoopNetty)
                                                     .asyncConfiguration(c -> c.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, Runnable::run))
                                                     .build();
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
