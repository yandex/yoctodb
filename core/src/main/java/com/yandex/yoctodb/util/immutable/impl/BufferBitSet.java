package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

public class BufferBitSet {
    public static boolean get(@NotNull final Buffer buf,
                              final long bufferOffset,
                              final int i) {
        assert bufferOffset <= buf.limit() - (i / Long.SIZE + 1) * Long.BYTES;

        final long word = i >> 6; // div 64
        final long bit = i & 0x3f; // mod 64
        final long mask = 1L << bit;

        return (buf.getLong(bufferOffset + word * Long.BYTES) & mask) != 0;
    }

    public static int nextSetBit(@NotNull final Buffer buf,
                                 final long bufferOffset,
                                 final int sizeHint,
                                 final int fromIndexInclusive) {
        assert fromIndexInclusive >= 0;
        assert bufferOffset <= buf.limit() - (fromIndexInclusive / Long.SIZE + 1) * Long.BYTES;

        if (fromIndexInclusive >= sizeHint) {
            return -1;
        }

        final int wordCount = arraySize(sizeHint);
        int u = fromIndexInclusive >> 6;
        long word = buf.getLong(bufferOffset + u * Long.BYTES);

        // Edge case
        if (word == ~0L)
            return fromIndexInclusive;

        word &= 0xffffffffffffffffL << fromIndexInclusive;

        while (true) {
            if (word != 0) {
                return (u << 6) | Long.numberOfTrailingZeros(word);
            }
            if (++u == wordCount) {
                return -1;
            }
            word = buf.getLong(bufferOffset + u * Long.BYTES);
        }
    }

    public static int cardinalityTo(@NotNull final Buffer buf,
                                    final long bufferOffset,
                                    final int i) {
        assert bufferOffset <= buf.limit() - (i / Long.SIZE + 1) * Long.BYTES;

        int result = 0;
        final int toWordIndex = i >> 6;
        int word = 0;
        while (word < toWordIndex) {
            result += Long.bitCount(buf.getLong(bufferOffset + word * Long.BYTES));
            word++;
        }

        final int bit = i & 0x3f;
        final long mask = (1L << bit) - 1;
        result += Long.bitCount(buf.getLong(bufferOffset + word * Long.BYTES) & mask);

        return result;
    }

    public static int arraySize(final int bitCount) {
        return (bitCount >>> 6) + ((bitCount & 0x3f) != 0 ? 1 : 0);
    }
}
