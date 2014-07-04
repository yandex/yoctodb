/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.util.immutable.impl;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import ru.yandex.yoctodb.util.immutable.IntToIntArray;
import ru.yandex.yoctodb.util.mutable.BitSet;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * @author svyatoslav
 */
@Immutable
public class BitSetBasedIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final int keysCount;
    @NotNull
    private final ByteBuffer elements;
    private final int bitSetSizeInLongs;
    private final int bitSetSizeInBytes;

    @NotNull
    public static IndexToIndexMultiMap from(
            @NotNull
            final ByteBuffer buf) {
        final int keysCount = buf.getInt();
        assert keysCount > 0;

        final int bitSetSizeInLongs = buf.getInt();

        final ByteBuffer elements = buf.slice();

        final int bitSetSizeInBytes = bitSetSizeInLongs << 3;

        return new BitSetBasedIndexToIndexMultiMap(
                keysCount,
                elements.slice(),
                bitSetSizeInLongs, bitSetSizeInBytes);
    }

    private BitSetBasedIndexToIndexMultiMap(
            final int keysCount,
            @NotNull
            final ByteBuffer elements,
            final int bitSetSizeInLongs, int bitSetSizeInBytes) {
        this.bitSetSizeInBytes = bitSetSizeInBytes;
        assert keysCount > 0;

        assert elements.hasRemaining();

        this.keysCount = keysCount;
        this.bitSetSizeInLongs = bitSetSizeInLongs;
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
        int remaining = elements.remaining();

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
