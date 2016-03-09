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
import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * {@link ThreadLocal}-based LIFO cache of {@link ArrayBitSet}s
 *
 * @author incubos
 */
@ThreadSafe
public final class ThreadLocalCachedArrayBitSetPool
        extends AbstractCachedArrayBitSetPool {
    private static class Cache extends ThreadLocal<Deque<long[]>> {
        @Override
        protected Deque<long[]> initialValue() {
            return new ArrayDeque<>();
        }
    }

    @NotNull
    private final ThreadLocal<Deque<long[]>> cache = new Cache();

    public ThreadLocalCachedArrayBitSetPool(
            final int sizeHint,
            final float loadFactor) {
        super(sizeHint, loadFactor);
    }

    public ThreadLocalCachedArrayBitSetPool() {
        this(DEFAULT_SIZE_HINT, DEFAULT_LOAD_FACTOR);
    }

    @Override
    protected Deque<long[]> getCache() {
        return cache.get();
    }
}
