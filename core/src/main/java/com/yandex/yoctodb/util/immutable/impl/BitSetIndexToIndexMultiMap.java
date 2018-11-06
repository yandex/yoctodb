/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.mutable.BitSet;

import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author svyatoslav
 */
@Immutable
public class BitSetIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final int keysCount;
    @NotNull
    private final Buffer elements;
    private final int bitSetSizeInLongs;
    private final long bitSetSizeInBytes;

    @NotNull
    public static BitSetIndexToIndexMultiMap from(
            @NotNull
            final Buffer buf) {
        final int keysCount = buf.getInt();
        final int bitSetSizeInLongs = buf.getInt();
        final Buffer elements = buf.slice();

        return new BitSetIndexToIndexMultiMap(
                keysCount,
                elements.slice(),
                bitSetSizeInLongs);
    }

    private BitSetIndexToIndexMultiMap(
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

    @Override
    public boolean get(
            @NotNull
            final BitSet dest,
            final int key) {
        assert 0 <= key && key < keysCount;

        final long start = key * bitSetSizeInBytes;

        return dest.or(elements, start, bitSetSizeInLongs);
    }

    @Override
    public boolean getFrom(
            @NotNull
            final BitSet dest,
            final int fromInclusive) {
        assert 0 <= fromInclusive && fromInclusive < keysCount;

        boolean result = false;

        long current = fromInclusive * bitSetSizeInBytes;
        final long remaining = elements.remaining();

        assert remaining <= Integer.MAX_VALUE;

        while (current < remaining) {
            result |= dest.or(elements, current, bitSetSizeInLongs);
            current += bitSetSizeInBytes;
        }

        return result;
    }

    @Override
    public boolean getTo(
            @NotNull
            final BitSet dest,
            final int toExclusive) {
        assert 0 < toExclusive && toExclusive <= keysCount;

        int remaining = toExclusive;

        boolean result = false;

        int current = 0;
        while (remaining > 0) {
            result |= dest.or(elements, current, bitSetSizeInLongs);
            current += bitSetSizeInBytes;
            remaining--;
        }

        return result;
    }

    @Override
    public boolean getBetween(
            @NotNull
            final BitSet dest,
            final int fromInclusive,
            final int toExclusive) {
        assert 0 <= fromInclusive &&
                fromInclusive < toExclusive &&
                toExclusive <= keysCount;

        int remaining = toExclusive - fromInclusive;
        long current = fromInclusive * bitSetSizeInBytes;
        boolean result = false;

        while (remaining > 0) {
            result |= dest.or(elements, current, bitSetSizeInLongs);
            current += bitSetSizeInBytes;
            remaining--;
        }

        return result;
    }

    @Override
    public int getKeysCount() {
        return keysCount;
    }

    @Override
    public String toString() {
        return "BitSetBasedIndexToIndexMultiMap{" +
                "keysCount=" + keysCount +
                '}';
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> ascending(
            @NotNull
            final BitSet valueFilter) {
        final ArrayBitSet bs = LongArrayBitSet.zero(valueFilter.getSize());
        return Stream.iterate(0, i -> i + 1)
                .map(i -> getIntToIntArray(valueFilter, bs, i))
                .limit(keysCount).iterator();
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> descending(
            @NotNull
            final BitSet valueFilter) {

        final ArrayBitSet bs = LongArrayBitSet.zero(valueFilter.getSize());
        return Stream.iterate(keysCount - 1, i -> i - 1)
                .map(i -> getIntToIntArray(valueFilter, bs, i))
                .limit(keysCount).iterator();
    }

    @NotNull
    private IntToIntArray getIntToIntArray(
            @NotNull
            final BitSet valueFilter,
            final ArrayBitSet dest,
            Integer i) {
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
