/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import com.yandex.yoctodb.util.mutable.impl.ThreadLocalCachedArrayBitSetPool;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * {@link IndexToIndexMultiMap} implementation based on accumulated {@link BitSet}s.
 * Main idea of this index is storing not the documents that satisfies key,
 * but only the ones which less than that key.
 *
 * Restriction: document with this index type must not provide more than one value.
 *
 * @author Andrey Korzinev (ya-goodfella@yandex.com)
 */
@Immutable
public class AscendingBitSetIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final int keysCount;
    @NotNull
    private final Buffer elements;
    private final int bitSetSizeInLongs;
    private final long bitSetSizeInBytes;

    private volatile BitSet nonNullBitSet; // lazy-evaluated
    private volatile ThreadLocalCachedArrayBitSetPool bitSetPool; // lazy-evaluated

    @NotNull
    public static AscendingBitSetIndexToIndexMultiMap from(
            @NotNull
            final Buffer buf) {
        final int keysCount = buf.getInt();
        final int bitSetSizeInLongs = buf.getInt();
        final Buffer elements = buf.slice();

        return new AscendingBitSetIndexToIndexMultiMap(
                keysCount,
                elements.slice(),
                bitSetSizeInLongs);
    }

    private AscendingBitSetIndexToIndexMultiMap(
            final int keysCount,
            @NotNull
            final Buffer elements,
            final int bitSetSizeInLongs) {
        assert keysCount >= 0 : "Negative keys count";
        this.keysCount = keysCount;
        this.bitSetSizeInLongs = bitSetSizeInLongs;
        this.bitSetSizeInBytes = ((long) bitSetSizeInLongs) << 3;
        this.elements = elements;
    }

    private ArrayBitSet borrowBitSet(int size) {
        ThreadLocalCachedArrayBitSetPool pool = bitSetPool;
        if (pool == null) {
            synchronized (this) {
                if (bitSetPool == null) {
                    bitSetPool = new ThreadLocalCachedArrayBitSetPool(bitSetSizeInLongs * Long.SIZE, 1.0f);
                }
            }

            return bitSetPool.borrowSet(size);
        }

        return pool.borrowSet(size);
    }

    private void release(@NotNull ArrayBitSet bitset) {
        bitSetPool.returnSet(bitset);
    }

    /**
     * @param sizeHint hint for bit set size
     * @return {@link BitSet} that contains every document that was associated with key (e.g. not null)
     */
    private BitSet getNonNull(int sizeHint) {
        BitSet result = nonNullBitSet;
        if (result == null) {
            synchronized (this) {
                if (nonNullBitSet == null) {
                    result = LongArrayBitSet.zero(sizeHint);
                    result.or(elements, keysCount * bitSetSizeInBytes, bitSetSizeInLongs);
                    nonNullBitSet = result;
                }
            }
        }

        assert result.getSize() == sizeHint;

        return result;
    }

    /**
     * Sets document bits for provided key index
     *
     * Complexity: O(1)
     * Additional space: O(1)
     *
     * @param dest destination {@link BitSet}
     * @param key key index
     * @return true if destination have non-zero bits afterwards, false otherwise
     */
    @Override
    public boolean get(
            @NotNull
            final BitSet dest,
            final int key) {
        assert 0 <= key && key < keysCount;

        return getBetween(dest, key, key + 1);
    }

    /**
     * Sets document bits for documents for keys which is greater or equals to key index
     *
     * Complexity: O(1)
     * Additional space: O(1)
     *
     * @param dest destination {@link BitSet}
     * @param fromInclusive lowest key index (inclusive)
     * @return true if destination have non-zero bits afterwards, false otherwise
     */
    @Override
    public boolean getFrom(
            @NotNull
            final BitSet dest,
            final int fromInclusive) {
        assert 0 <= fromInclusive && fromInclusive < keysCount;

        return getBetween(dest, fromInclusive, keysCount);
    }

    /**
     * Sets document bits for documents for keys which is less than key index
     *
     * Complexity: O(1)
     * Additional space: -
     *
     * @param dest destination {@link BitSet}
     * @param toExclusive highest key index (exclusive)
     * @return true if destination have non-zero bits afterwards, false otherwise
     */
    @Override
    public boolean getTo(
            @NotNull
            final BitSet dest,
            final int toExclusive) {
        assert 0 < toExclusive && toExclusive <= keysCount;

        // edge case optimization
        if (toExclusive == keysCount) {
            return dest.or(getNonNull(dest.getSize()));
        }

        return dest.or(elements, toExclusive * bitSetSizeInBytes, bitSetSizeInLongs);
    }

    /**
     * Sets document bits for documents for keys which is between selected keys
     *
     * Complexity: O(1)
     * Additional space: O(1)
     *
     * @param dest destination {@link BitSet}
     * @param fromInclusive lowest key index (inclusive)
     * @param toExclusive highest key index (exclusive)
     * @return true if destination have non-zero bits afterwards, false otherwise
     */
    @Override
    public boolean getBetween(
            @NotNull
            final BitSet dest,
            final int fromInclusive,
            final int toExclusive) {
        assert 0 <= fromInclusive &&
                fromInclusive < toExclusive &&
                toExclusive <= keysCount;

        final ArrayBitSet target = borrowBitSet(dest.getSize());

        try {
            getTo(target, toExclusive);

            // edge case optimization
            if (fromInclusive != 0) {
                target.xor(elements, fromInclusive * bitSetSizeInBytes, bitSetSizeInLongs);
            }

            return dest.or(target);
        } finally {
            release(target);
        }
    }

    @Override
    public int getKeysCount() {
        return keysCount;
    }

    @Override
    public String toString() {
        return "AscendingBitSetIndexToIndexMultiMap{" +
                "keysCount=" + keysCount +
                '}';
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> ascending(
            @NotNull
            final BitSet valueFilter) {
        final ArrayBitSet bs = LongArrayBitSet.zero(valueFilter.getSize());
        return IntStream.iterate(0, i -> i + 1)
                .mapToObj(i -> getIntToIntArray(valueFilter, bs, i))
                .limit(keysCount)
                .iterator();
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> descending(
            @NotNull
            final BitSet valueFilter) {
        final ArrayBitSet bs = LongArrayBitSet.zero(valueFilter.getSize());
        return IntStream.iterate(keysCount - 1, i -> i - 1)
                .mapToObj(i -> getIntToIntArray(valueFilter, bs, i))
                .limit(keysCount)
                .iterator();
    }

    @NotNull
    private IntToIntArray getIntToIntArray(
            @NotNull
            final BitSet valueFilter,
            final ArrayBitSet dest,
            final int i) {
        dest.clear();
        get(dest, i);
        dest.and(valueFilter);
        final int count = dest.cardinality();

        return new IntToIntArray(
                i,
                IntStream.iterate(dest.nextSetBit(0), b -> dest.nextSetBit(b + 1))
                        .limit(count)
                        .toArray(),
                count
        );
    }
}
