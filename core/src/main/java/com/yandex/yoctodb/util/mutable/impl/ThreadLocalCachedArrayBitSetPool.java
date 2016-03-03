/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * {@link ThreadLocal}-based LIFO cache of {@link ArrayBitSet}s
 *
 * @author incubos
 */
public final class ThreadLocalCachedArrayBitSetPool implements ArrayBitSetPool {
    public static final int DEFAULT_SIZE_HINT = 1;
    public static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private final int minSize;
    private final float loadFactor;

    private static class Cache extends ThreadLocal<Deque<long[]>> {
        @Override
        protected Deque<long[]> initialValue() {
            return new ArrayDeque<long[]>();
        }
    }

    @NotNull
    private final ThreadLocal<Deque<long[]>> cache = new Cache();

    private static int sizeToAllocate(final int bits, final float loadFactor) {
        return (int) (LongArrayBitSet.arraySize(bits) / loadFactor);
    }

    public ThreadLocalCachedArrayBitSetPool(
            final int sizeHint,
            final float loadFactor) {
        assert sizeHint > 0;
        assert 0.0f < loadFactor && loadFactor <= 1.0f;

        this.minSize = sizeToAllocate(sizeHint, loadFactor);
        this.loadFactor = loadFactor;
    }

    public ThreadLocalCachedArrayBitSetPool() {
        this(DEFAULT_SIZE_HINT, DEFAULT_LOAD_FACTOR);
    }

    @NotNull
    private long[] allocate(final int size) {
        final int actualSize =
                Math.max(sizeToAllocate(size, loadFactor), minSize);
        return new long[actualSize];
    }

    @NotNull
    @Override
    public ArrayBitSet borrowSet(final int size) {
        assert size > 0;

        final Deque<long[]> lifo = cache.get();
        final long[] cached = lifo.poll();
        if (cached == null)
            return LongArrayBitSet.zero(size, allocate(size));
        else if (cached.length < LongArrayBitSet.arraySize(size))
            return LongArrayBitSet.zero(size, allocate(size));
        else
            return LongArrayBitSet.zero(size, cached);
    }

    @Override
    public void returnSet(
            @NotNull
            final ArrayBitSet set) {
        cache.get().addFirst(set.toArray());
    }
}
