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
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

/**
 * {@link BitSet} factory with an upper bound on the number of sets constructed
 * and some tools to extract all the constructed bitset, but only once
 *
 * @author incubos
 */
public final class BoundedBitSetPool implements BitSetPool {
    private final int bitSetSize;
    @NotNull
    private final Deque<BitSet> free;
    @NotNull
    private final Deque<BitSet> used;
    private int maxBitSets;

    protected BoundedBitSetPool(
            final int bitSetSize,
            @NotNull
            final Deque<BitSet> free,
            final int maxBitSets) {
        assert maxBitSets > 0;
        assert free.size() <= maxBitSets;

        this.bitSetSize = bitSetSize;
        this.free = free;
        this.used = new ArrayDeque<BitSet>();
        this.maxBitSets = maxBitSets;
    }

    @Override
    public int getBitSetSize() {
        return bitSetSize;
    }

    @NotNull
    protected Deque<BitSet> popUsedBitSets() {
        if (used.isEmpty() && free.isEmpty() && maxBitSets == 0)
            throw new IllegalStateException("Double pop");

        final Deque<BitSet> result =
                new ArrayDeque<BitSet>(used.size() + free.size());
        result.addAll(used);
        result.addAll(free);

        // Contract
        used.clear();
        free.clear();
        maxBitSets = 0;

        return result;
    }

    @NotNull
    @Override
    public BitSet borrowSet() {
        if (maxBitSets == 0)
            throw new NoSuchElementException("Quota reached");

        final BitSet cached = free.poll();
        final BitSet result;
        if (cached == null) {
            result = LongArrayBitSet.zero(bitSetSize);
        } else {
            result = cached;
        }

        used.push(result);
        maxBitSets--;

        return result;
    }

    @Override
    public void returnSet(
            @NotNull
            final BitSet set) {
        assert set.getSize() == getBitSetSize();

        used.remove(set);
        free.push(set);
        maxBitSets++;
    }
}
