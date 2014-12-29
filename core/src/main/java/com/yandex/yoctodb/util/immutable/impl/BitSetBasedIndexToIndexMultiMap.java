/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.mutable.BitSet;

import java.util.Iterator;

/**
 * @author svyatoslav
 */
@Immutable
public class BitSetBasedIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final int keysCount;
    @NotNull
    private final Buffer elements;
    private final int bitSetSizeInLongs;
    private final int bitSetSizeInBytes;

    @NotNull
    public static IndexToIndexMultiMap from(
            @NotNull
            final Buffer buf) {
        final int keysCount = buf.getInt();
        final int bitSetSizeInLongs = buf.getInt();
        final Buffer elements = buf.slice();
        final int bitSetSizeInBytes = bitSetSizeInLongs << 3;

        return new BitSetBasedIndexToIndexMultiMap(
                keysCount,
                elements.slice(),
                bitSetSizeInLongs,
                bitSetSizeInBytes);
    }

    private BitSetBasedIndexToIndexMultiMap(
            final int keysCount,
            @NotNull
            final Buffer elements,
            final int bitSetSizeInLongs,
            final int bitSetSizeInBytes) {
        if (keysCount <= 0)
            throw new IllegalArgumentException("No keys");
        if (!elements.hasRemaining())
            throw new IllegalArgumentException("No elements");

        this.keysCount = keysCount;
        this.bitSetSizeInLongs = bitSetSizeInLongs;
        this.bitSetSizeInBytes = bitSetSizeInBytes;
        this.elements = elements;
    }

    @Override
    public boolean get(
            @NotNull
            final BitSet dest,
            final int key) {
        assert 0 <= key && key < keysCount;

        final int start = key * bitSetSizeInBytes;
        return dest.or(elements, start, bitSetSizeInLongs);
    }

    @Override
    public boolean getFrom(
            @NotNull
            final BitSet dest,
            final int fromInclusive) {
        assert 0 <= fromInclusive && fromInclusive < keysCount;

        boolean result = false;

        int current = fromInclusive * bitSetSizeInBytes;
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
        int current = fromInclusive * bitSetSizeInBytes;
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
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> descending(
            @NotNull
            final BitSet valueFilter) {
        throw new UnsupportedOperationException();
    }
}
