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

package software.amazon.awssdk.http.nio.netty.internal;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.metrics.MetricCollector;

@SdkInternalApi
public final class SimpleChannelPoolAwareChannelPool implements SdkChannelPool {
    private final SdkChannelPool delegate;
    private final BetterSimpleChannelPool simpleChannelPool;
    private final EventLoop eventLoop;

    SimpleChannelPoolAwareChannelPool(SdkChannelPool delegate,
                                      BetterSimpleChannelPool simpleChannelPool,
                                      EventLoop eventLoop) {
        this.delegate = delegate;
        this.simpleChannelPool = simpleChannelPool;
        this.eventLoop = eventLoop;
    }

    @Override
    public Future<Channel> acquire() {
        return delegate.acquire();
    }

    @Override
    public Future<Channel> acquire(Promise<Channel> promise) {
        return delegate.acquire(promise);
    }

    @Override
    public Future<Void> release(Channel channel) {
        return delegate.release(channel);
    }

    @Override
    public Future<Void> release(Channel channel, Promise<Void> promise) {
        return delegate.release(channel, promise);
    }

    @Override
    public void close() {
        delegate.close();
    }

    public EventLoop eventLoop() {
        return eventLoop;
    }

    public BetterSimpleChannelPool underlyingSimpleChannelPool() {
        return simpleChannelPool;
    }

    @Override
    public CompletableFuture<Void> collectChannelPoolMetrics(MetricCollector metrics) {
        return delegate.collectChannelPoolMetrics(metrics);
    }
}
