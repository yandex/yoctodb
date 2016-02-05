/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.BitSet;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * {@link BitSet} implementation based on {@code long} array.
 *
 * Based on Lucene {@code FixedBitSet}.
 *
 * @author incubos
 */
@NotThreadSafe
public final class LongArrayBitSet implements ArrayBitSet {
    @NotNull
    private final long[] words;
    private final int size;

    private LongArrayBitSet(final int size) {
        assert size > 0;

        this.size = size;
        this.words = new long[arraySize(size)];
    }

    public static int arraySize(final int size) {
        return (size >>> 6) + 1;
    }

    public static ArrayBitSet one(final int size) {
        final LongArrayBitSet result = new LongArrayBitSet(size);
        // Filling with ones
        Arrays.fill(result.words, 0, result.words.length - 1, -1L);
        final int shift = size & 0x3f;
        if (shift != 0) {
            result.words[result.words.length - 1] = ~(-1L << shift);
        }

        return result;
    }

    public static ArrayBitSet zero(final int size) {
        return new LongArrayBitSet(size);
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public int cardinality() {
        int result = 0;
        for (long w : words) {
            result += Long.bitCount(w);
        }

        return result;
    }

    @NotNull
    @Override
    public long[] toArray() {
        return words;
    }

    @Override
    public void set(final int i) {
        assert 0 <= i && i < size;
        final int word = i >> 6;      // div 64
        final int bit = i & 0x3f;     // mod 64
        final long mask = 1L << bit;
        words[word] |= mask;
    }

    @Override
    public boolean get(final int i) {
        assert 0 <= i && i < size;
        final int word = i >> 6;      // div 64
        final int bit = i & 0x3f;     // mod 64
        final long mask = 1L << bit;
        return (words[word] & mask) != 0;
    }

    public void clear() {
        Arrays.fill(words, 0L);
    }

    @Override
    public boolean inverse() {
        boolean notEmpty = false;

        // Inverse all the words except last one
        for (int i = 0; i < words.length - 1; i++) {
            words[i] = ~words[i];
            if (words[i] != 0)
                notEmpty = true;
        }

        // Fix bits in last word
        final int shift = size & 0x3f;
        if (shift != 0) {
            final int last = words.length - 1;
            words[last] = ~words[last] & ~(-1L << shift);

            if (words[last] != 0)
                notEmpty = true;
        }

        return notEmpty;
    }

    public void set() {
        // Filling with ones
        Arrays.fill(words, 0, words.length - 1, -1L);
        final int shift = size & 0x3f;
        if (shift != 0) {
            words[words.length - 1] = ~(-1L << shift);
        }
    }

    @Override
    public boolean and(
            @NotNull
            final BitSet set) {
        assert size == set.getSize();

        boolean notEmpty = false;
        final long[] from = ((ArrayBitSet) set).toArray();
        for (int i = 0; i < words.length; i++) {
            final long word = words[i] & from[i];
            words[i] = word;
            if (word != 0) {
                notEmpty = true;
            }
        }

        return notEmpty;
    }

    @Override
    public boolean or(
            @NotNull
            final BitSet set) {
        assert size == set.getSize();

        boolean notEmpty = false;
        final long[] from = ((ArrayBitSet) set).toArray();
        for (int i = 0; i < words.length; i++) {
            final long word = words[i] | from[i];
            words[i] = word;
            if (word != 0) {
                notEmpty = true;
            }
        }

        return notEmpty;
    }

    @Override
    public boolean or(
            @NotNull
            final Buffer longArrayBitSetInByteBuffer,
            final long startPosition,
            final int bitSetSizeInLongs) {
        boolean notEmpty = false;
        long currentPosition = startPosition;

        assert words.length == bitSetSizeInLongs;

        for (int i = 0; i < words.length; i++) {
            final long currentWord =
                    longArrayBitSetInByteBuffer.getLong(
                            currentPosition);
            currentPosition += Long.BYTES;
            final long word = words[i] | currentWord;
            words[i] = word;
            if (word != 0) {
                notEmpty = true;
            }
        }

        return notEmpty;
    }

    @Override
    public boolean isEmpty() {
        for (long w : words)
            if (w != 0) {
                return false;
            }

        return true;
    }

    /**
     * See {@link java.util.BitSet#nextSetBit(int)}
     */
    @Override
    public int nextSetBit(final int fromIndexInclusive) {
        assert 0 <= fromIndexInclusive;

        if (fromIndexInclusive >= size) {
            return -1;
        }

        int u = fromIndexInclusive >> 6;
        long word = words[u];

        // Edge case
        if (word == ~0L)
            return fromIndexInclusive;

        word &= 0xffffffffffffffffL << fromIndexInclusive;

        while (true) {
            if (word != 0) {
                return (u << 6) | Long.numberOfTrailingZeros(word);
            }
            if (++u == words.length) {
                return -1;
            }
            word = words[u];
        }
    }
}
