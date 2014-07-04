/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.util.immutable.impl;

import com.google.common.primitives.Ints;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import ru.yandex.yoctodb.util.immutable.IntToIntArray;
import ru.yandex.yoctodb.util.mutable.BitSet;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * @author svyatoslav
 */
@Immutable
public final class IntIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final int keysCount;
    @NotNull
    private final ByteBuffer offsets;
    @NotNull
    private final ByteBuffer elements;

    @NotNull
    public static IndexToIndexMultiMap from(
            @NotNull
            final ByteBuffer buf) {
        final int keysCount = buf.getInt();
        assert keysCount > 0;

        final ByteBuffer offsets = buf.slice();
        offsets.limit(4 * keysCount);

        final ByteBuffer elements = buf.slice();
        elements.position(offsets.remaining());

        return new IntIndexToIndexMultiMap(
                keysCount,
                offsets.slice(),
                elements.slice());
    }

    private IntIndexToIndexMultiMap(
            final int keysCount,
            @NotNull
            final ByteBuffer offsets,
            @NotNull
            final ByteBuffer elements) {
        assert keysCount > 0;
        assert offsets.hasRemaining();
        assert elements.hasRemaining();

        this.keysCount = keysCount;
        this.offsets = offsets;
        this.elements = elements;
    }

    @Override
    public boolean get(
            @NotNull
            final BitSet dest,
            final int key) {
        assert 0 <= key && key < keysCount;

        final int start = offsets.getInt(key << 2);

        final int size = elements.getInt(start);
        final int from = start + 4;
        final int to = from + (size << 2);
        for (int i = from; i < to; i += 4)
            dest.set(elements.getInt(i));

        return size > 0;
    }

    @Override
    public boolean getFrom(
            @NotNull
            final BitSet dest,
            final int fromInclusive) {
        assert 0 <= fromInclusive && fromInclusive < keysCount;

        boolean result = false;

        int current = offsets.getInt(fromInclusive << 2);
        int remaining = elements.remaining();

        while (current < remaining) {
            int size = elements.getInt(current);
            current += 4;
            result |= size > 0;
            for (; 0 < size; size--) {
                dest.set(elements.getInt(current));
                current += 4;
            }
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
            int size = elements.getInt(current);
            current += 4;
            result |= size > 0;
            for (; 0 < size; size--) {
                dest.set(elements.getInt(current));
                current += 4;
            }
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

        int current = offsets.getInt(fromInclusive << 2);
        int remaining = toExclusive - fromInclusive;

        boolean result = false;

        while (remaining > 0) {
            int size = elements.getInt(current);
            current += 4;
            result |= size > 0;
            for (; 0 < size; size--) {
                dest.set(elements.getInt(current));
                current += 4;
            }
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
        return "IntIndexToIndexMultiMap{" +
               "keysCount=" + keysCount +
               '}';
    }

    @Nullable
    private IntToIntArray getFilteredValues(
            final int key,
            @NotNull
            final BitSet valueFilter) {
        assert 0 <= key && key < keysCount;

        final int start = offsets.getInt(key << 2);

        final int size = elements.getInt(start);

        assert size > 0;

        int[] values = null;
        int count = 0;

        int valueOffset = start + 4;
        for (int i = 0; i < size; i++) {
            final int value = elements.getInt(valueOffset);
            if (valueFilter.get(value)) {
                // Lazy allocation
                if (values == null) {
                    values = new int[size - i];
                }
                values[count] = value;
                count++;
            }
            valueOffset += 4;
        }

        if (values == null) {
            return null;
        } else {
            return new IntToIntArray(key, values, count);
        }
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> ascending(
            @NotNull
            final BitSet valueFilter) {
        return new Iterator<IntToIntArray>() {
            private int key = 0;
            private IntToIntArray next = null;

            private void advance() {
                while (next == null && key < keysCount) {
                    next = getFilteredValues(key++, valueFilter);
                }
            }

            @Override
            public boolean hasNext() {
                if (next != null)
                    return true;

                advance();

                return next != null;
            }

            @Override
            public IntToIntArray next() {
                assert next != null;

                final IntToIntArray result = next;
                next = null;

                advance();

                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException(
                        "Removal not supported");
            }
        };
    }

    @NotNull
    @Override
    public Iterator<IntToIntArray> descending(
            @NotNull
            final BitSet valueFilter) {
        return new Iterator<IntToIntArray>() {
            private int key = keysCount - 1;
            private IntToIntArray next = null;

            private void advance() {
                while (next == null && key >= 0) {
                    next = getFilteredValues(key--, valueFilter);
                }
            }

            @Override
            public boolean hasNext() {
                if (next != null)
                    return true;

                advance();

                return next != null;
            }

            @Override
            public IntToIntArray next() {
                assert next != null;

                final IntToIntArray result = next;
                next = null;

                advance();

                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException(
                        "Removal not supported");
            }
        };
    }
}
