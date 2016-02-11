/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.query;

import com.yandex.yoctodb.query.BitSetPool;
import com.yandex.yoctodb.query.BitSetPoolPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Thread local cached {@link BoundedBitSetPool} pool
 *
 * @author incubos
 */
public final class ThreadLocalCachedBitSetPoolPool
        implements BitSetPoolPool {
    private final int bitSetSize;
    private final int maxBitSets;
    private final ThreadLocal<Deque<BitSet>> cache =
            new ThreadLocal<Deque<BitSet>>() {
                @Override
                protected Deque<BitSet> initialValue() {
                    return new ArrayDeque<BitSet>();
                }
            };

    public ThreadLocalCachedBitSetPoolPool(
            final int bitSetSize,
            final int maxBitSets) {
        assert maxBitSets > 0;

        this.bitSetSize = bitSetSize;
        this.maxBitSets = maxBitSets;
    }

    @NotNull
    @Override
    public BitSetPool borrowPool() {
        final Deque<BitSet> sets = cache.get();
        if (sets == null)
            throw new IllegalStateException("The resource is already busy");

        cache.set(null);

        return new BoundedBitSetPool(bitSetSize, sets, maxBitSets);
    }

    @Override
    public void returnPool(
            @NotNull
            final BitSetPool pool) {
        assert cache.get() == null;

        final BoundedBitSetPool specific = (BoundedBitSetPool) pool;

        cache.set(specific.popUsedBitSets());
    }
}
