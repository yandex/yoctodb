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
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.mutable.BitSet;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author incubos
 * @see com.yandex.yoctodb.util.mutable.impl.RoaringBitSetIndexToIndexMultiMap
 */
@Immutable
public class RoaringBitSetIndexToIndexMultiMap implements IndexToIndexMultiMap {
    @NotNull
    private final List<ImmutableRoaringBitmap> bitSets;

    @NotNull
    public static RoaringBitSetIndexToIndexMultiMap from(
            @NotNull
            final Buffer buf) {
        final Collection<ImmutableRoaringBitmap> bitSets = new LinkedList<>();
        while (buf.hasRemaining()) {
            final int size = buf.getInt();
            final ByteBuffer data = buf.slice(size).toByteBuffer();
            bitSets.add(new ImmutableRoaringBitmap(data));
            buf.advance(size);
        }

        return new RoaringBitSetIndexToIndexMultiMap(bitSets);
    }

    private RoaringBitSetIndexToIndexMultiMap(
            @NotNull
            final Collection<ImmutableRoaringBitmap> bitSets) {
        this.bitSets = new ArrayList<>(bitSets);
    }

    @Override
    public boolean get(
            @NotNull
            final BitSet dest,
            final int key) {
        assert 0 <= key && key < bitSets.size();

        final Iterator<Integer> docs = bitSets.get(key).iterator();
        final boolean result = docs.hasNext();
        while (docs.hasNext()) {
            dest.set(docs.next());
        }

        return result;
    }

    @Override
    public boolean getFrom(
            @NotNull
            final BitSet dest,
            final int fromInclusive) {
        assert 0 <= fromInclusive && fromInclusive < bitSets.size();

        boolean result = false;

        for (int i = fromInclusive; i < bitSets.size(); i++) {
            final boolean nonEmpty = get(dest, i);
            result |= nonEmpty;
        }

        return result;
    }

    @Override
    public boolean getTo(
            @NotNull
            final BitSet dest,
            final int toExclusive) {
        assert 0 < toExclusive && toExclusive <= bitSets.size();

        boolean result = false;

        for (int i = 0; i < toExclusive; i++) {
            final boolean nonEmpty = get(dest, i);
            result |= nonEmpty;
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
               toExclusive <= bitSets.size();

        boolean result = false;

        for (int i = fromInclusive; i < toExclusive; i++) {
            final boolean nonEmpty = get(dest, i);
            result |= nonEmpty;
        }

        return result;
    }

    @Override
    public int getKeysCount() {
        return bitSets.size();
    }

    @Override
    public String toString() {
        return "RoaringBitSetIndexToIndexMultiMap{" +
               "size=" + bitSets.size() +
               '}';
    }

    @Nullable
    private IntToIntArray getFilteredValues(
            final int key,
            @NotNull
            final BitSet valueFilter) {
        assert 0 <= key && key < bitSets.size();
        assert !valueFilter.isEmpty();

        int[] values = null;
        int count = 0;

        final ImmutableRoaringBitmap bitSet = bitSets.get(key);
        final PeekableIntIterator ids = bitSet.getIntIterator();

        int nextAllowed = valueFilter.nextSetBit(0);
        ids.advanceIfNeeded(nextAllowed);

        // Contract: ids is always >= nextAllowed
        while (ids.hasNext()) {
            int id = ids.peekNext();

            if (id == nextAllowed) {
                // Lazy allocation
                if (values == null) {
                    values = new int[bitSet.getCardinality()];
                }

                // Storing
                values[count] = id;
                count++;

                // Advancing
                id++;
            }

            nextAllowed = valueFilter.nextSetBit(id);
            if (nextAllowed < 0)
                break;

            ids.advanceIfNeeded(nextAllowed);
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
                while (next == null && key < bitSets.size()) {
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
                if (!hasNext())
                    throw new NoSuchElementException();

                final IntToIntArray result = next;
                next = null;

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
            private int key = bitSets.size() - 1;
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
                if (!hasNext())
                    throw new NoSuchElementException();

                final IntToIntArray result = next;
                next = null;

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
