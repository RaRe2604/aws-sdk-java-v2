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

import static software.amazon.awssdk.utils.Validate.paramNotNull;

import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolMap;
import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.utils.Pair;

/**
 * Replacement for {@link io.netty.channel.pool.AbstractChannelPoolMap}. This implementation guarantees
 * only one instance of a {@link ChannelPool} is created for each key.
 */
// TODO do we need to use this for H2?
@SdkInternalApi
public abstract class SdkChannelPoolMap<K, P extends SimpleChannelPoolAwareChannelPool>
        implements ChannelPoolMap<K, P>, Iterable<Map.Entry<K, P>>, Closeable {

    private final ConcurrentMap<K, List<P>> poolsMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Pair<K, Thread>, P> map = new ConcurrentHashMap<>();

    @Override
    public final P get(K key) {
        return map.computeIfAbsent(Pair.of(key, Thread.currentThread()), this::createPools);
    }

    private P createPools(Pair<K, Thread> key) {
        List<P> pools = poolsMap.computeIfAbsent(key.left(), this::newPool);
        for (P pool : pools) {
            if (pool.eventLoop().inEventLoop()) {
                return pool;
            }
        }
        return pools.get(ThreadLocalRandom.current().nextInt(0, pools.size() - 1));
    }

    /**
     * Remove the {@link ChannelPool} from this {@link io.netty.channel.pool.AbstractChannelPoolMap}. Returns {@code true} if
     * removed, {@code false} otherwise.
     *
     * Please note that {@code null} keys are not allowed.
     */
    public final boolean remove(K key) {
        // P pool = poolsMap.remove(paramNotNull(key, "key"));
        // if (pool != null) {
        //     pool.close();
        //     return true;
        // }
        return false;
    }

    @Override
    public final Iterator<Map.Entry<K, P>> iterator() {
        throw new UnsupportedOperationException();
        // return new ReadOnlyIterator<>(poolsMap.entrySet().iterator());
    }

    /**
     * Returns the number of {@link ChannelPool}s currently in this {@link io.netty.channel.pool.AbstractChannelPoolMap}.
     */
    public final int size() {
        return poolsMap.size();
    }

    /**
     * Returns {@code true} if the {@link io.netty.channel.pool.AbstractChannelPoolMap} is empty, otherwise {@code false}.
     */
    public final boolean isEmpty() {
        return poolsMap.isEmpty();
    }

    @Override
    public final boolean contains(K key) {
        return poolsMap.containsKey(paramNotNull(key, "key"));
    }

    /**
     * Called once a new {@link ChannelPool} needs to be created as non exists yet for the {@code key}.
     */
    protected abstract List<P> newPool(K key);

    @Override
    public void close() {
        poolsMap.keySet().forEach(this::remove);
    }

    // public final Map<K, P> pools() {
    //     return Collections.unmodifiableMap(new HashMap<>(poolsMap));
    // }

    /**
     * {@link Iterator} that prevents removal.
     *
     * @param <T> Type of object being iterated.
     */
    private final class ReadOnlyIterator<T> implements Iterator<T> {
        private final Iterator<? extends T> iterator;

        private ReadOnlyIterator(Iterator<? extends T> iterator) {
            this.iterator = paramNotNull(iterator, "iterator");
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public T next() {
            return this.iterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Read-only iterator doesn't support removal.");
        }
    }
}

