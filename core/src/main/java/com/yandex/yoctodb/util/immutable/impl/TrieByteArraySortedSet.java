package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.common.BufferIterator;
import com.yandex.yoctodb.util.immutable.ByteArraySortedSet;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

import static com.yandex.yoctodb.util.common.TrieNodeMetadata.*;

public class TrieByteArraySortedSet implements ByteArraySortedSet {
    private final int keysCount;
    private final Buffer nodes;

    public static TrieByteArraySortedSet from(
            @NotNull final Buffer buffer) {
        final int size = buffer.getInt();
        final Buffer nodes = buffer.slice();

        return new TrieByteArraySortedSet(
                nodes,
                size);
    }

    private TrieByteArraySortedSet(@NotNull Buffer nodes,
                                   int keysCount) {
        this.keysCount = keysCount;
        this.nodes = nodes;
    }

    @Override
    public int size() {
        return keysCount;
    }

    @NotNull
    @Override
    public Buffer get(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(@NotNull Buffer e) {
        if (size() > 0) {
            return indexOf(new BufferIterator(e));
        }

        return -1;
    }

    @Override
    public int indexOfGreaterThan(@NotNull Buffer e, boolean orEquals, int upToIndexInclusive) {
        if (size() > 0) {
            int result = indexOfGreaterThan(new BufferIterator(e), orEquals);
            return result == keysCount ? -1 : result;
        }

        return -1;
    }

    @Override
    public int indexOfLessThan(@NotNull Buffer e, boolean orEquals, int fromIndexInclusive) {
        if (size() > 0) {
            return Math.max(-1, indexOfLessThan(new BufferIterator(e), orEquals));
        }

        return -1;
    }

    private int indexOf(@NotNull final Iterator<Byte> query) {
        long movingOffset = 0L;

        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if (hasInfix(metadata)) { // there is an infix
                int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                Iterator<Byte> infix = new BufferIterator(nodes, movingOffset, infixSize);
                if (BufferIterator.strip(query, infix) != 0) {
                    return -1;
                }
                movingOffset += infixSize * Byte.BYTES;
            }

            int maybeValue = -1;
            if (hasValue(metadata)) { // there is a value
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!query.hasNext()) {
                return maybeValue;
            }

            int next = Byte.toUnsignedInt(query.next());
            switch (edgeType(metadata)) {
                case EDGES_SINGLE:
                    if (next != Byte.toUnsignedInt(nodes.get(movingOffset++))) {
                        return -1;
                    }
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                case EDGES_BITSET: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    if (min > next || max < next || !BufferBitSet.get(nodes, movingOffset, next - min)) {
                        return -1;
                    }
                    int index = BufferBitSet.cardinalityTo(nodes, movingOffset, next - min);
                    movingOffset += BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
                    break;
                }
                case EDGES_CONDENSED: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    if (min > next || max < next) {
                        return -1;
                    }
                    int index = next - min;
                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
                    break;
                }
                case EDGES_NONE:
                    return -1;
            }
        }
    }

    private int takeFirstValueOnLeft(long movingOffset) {
        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if (hasInfix(metadata)) { // there is an infix
                int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                movingOffset += infixSize * Byte.BYTES;
            }

            if (hasValue(metadata)) { // there is a value
                return nodes.getInt(movingOffset);
            }

            switch (edgeType(metadata)) {
                case EDGES_BITSET:
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));

                    movingOffset += BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                case EDGES_CONDENSED:
                    movingOffset += 2 * Byte.BYTES;
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                default: // should be unreachable
            }
        }
    }

    private int takeLastValueOnRight(long movingOffset) {
        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if (hasInfix(metadata)) { // there is an infix
                int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                movingOffset += infixSize * Byte.BYTES;
            }

            int value = -1;
            if (hasValue(metadata)) { // there is a value
                value = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            switch (edgeType(metadata)) {
                case EDGES_SINGLE:
                    movingOffset += Byte.BYTES;
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                case EDGES_BITSET: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));

                    int index = BufferBitSet.cardinalityTo(nodes, movingOffset, max - min);
                    movingOffset += BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
                    break;
                }
                case EDGES_CONDENSED: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));

                    movingOffset = nodes.getLong(movingOffset + (max - min) * Long.BYTES);
                    break;
                }
                case EDGES_NONE:
                    return value;
            }
        }
    }

    private int indexOfGreaterThan(@NotNull final Iterator<Byte> query, final boolean orEquals) {
        long movingOffset = 0L;
        long nodeOffset;

        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            nodeOffset = movingOffset;
            movingOffset += Byte.BYTES;

            if (hasInfix(metadata)) { // there is an infix
                int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                Iterator<Byte> infix = new BufferIterator(nodes, movingOffset, infixSize);
                int infixCompare = BufferIterator.strip(query, infix);
                if (infixCompare > 0) { // query > node
                    return takeLastValueOnRight(nodeOffset);
                } else if (infixCompare < 0) { // infix > query
                    return takeFirstValueOnLeft(nodeOffset);
                }
                movingOffset += infixSize * Byte.BYTES;
            }

            int maybeValue = -1;
            if (hasValue(metadata)) { // there is a value
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!query.hasNext()) {
                return orEquals && maybeValue != -1 ? maybeValue : maybeValue + 1;
            }

            int next = Byte.toUnsignedInt(query.next());
            switch (edgeType(metadata)) {
                case EDGES_SINGLE:
                    int key = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    if (next > key) {
                        return takeLastValueOnRight(nodes.getLong(movingOffset)) + 1;
                    } else if (next < key) {
                        return takeFirstValueOnLeft(nodes.getLong(movingOffset));
                    }
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                case EDGES_BITSET: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    long bitsetOffset = movingOffset;
                    long bitsetSize = BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                    if (next < min) {
                        return takeFirstValueOnLeft(nodes.getLong(bitsetOffset + bitsetSize));
                    } else if (next > max) {
                        return takeLastValueOnRight(nodeOffset) + 1;
                    }

                    int index = BufferBitSet.cardinalityTo(nodes, bitsetOffset, next - min);
                    movingOffset += bitsetSize;

                    if (!BufferBitSet.get(nodes, bitsetOffset, next - min)) {
                        return takeFirstValueOnLeft(nodes.getLong(movingOffset + index * Long.BYTES));
                    }

                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
                    break;
                }
                case EDGES_CONDENSED: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));

                    if (next < min) {
                        return takeFirstValueOnLeft(nodes.getLong(movingOffset));
                    } else if (next > max) {
                        return takeLastValueOnRight(nodeOffset) + 1;
                    }

                    int index = next - min;
                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
                    break;
                }
                case EDGES_NONE:
                    return maybeValue + 1;
            }
        }
    }

    private int indexOfLessThan(@NotNull final Iterator<Byte> query, final boolean orEquals) {
        long movingOffset = 0L;
        long nodeOffset;

        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            nodeOffset = movingOffset;
            movingOffset += Byte.BYTES;

            if (hasInfix(metadata)) { // there is an infix
                int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                Iterator<Byte> infix = new BufferIterator(nodes, movingOffset, infixSize);
                int infixCompare = BufferIterator.strip(query, infix);
                if (infixCompare > 0) { // query > node
                    return takeLastValueOnRight(nodeOffset);
                } else if (infixCompare < 0) { // infix > query
                    return takeFirstValueOnLeft(nodeOffset) - 1;
                }
                movingOffset += infixSize * Byte.BYTES;
            }

            int maybeValue = -1;
            if (hasValue(metadata)) { // there is a value
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!query.hasNext()) {
                if (maybeValue != -1) {
                    return orEquals ? maybeValue : maybeValue - 1;
                }
                return takeFirstValueOnLeft(nodeOffset) - 1;
            }

            int next = Byte.toUnsignedInt(query.next());
            switch (edgeType(metadata)) {
                case EDGES_SINGLE:
                    int key = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    if (next > key) {
                        return takeLastValueOnRight(nodes.getLong(movingOffset));
                    } else if (next < key) {
                        return takeFirstValueOnLeft(nodes.getLong(movingOffset)) - 1;
                    }
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                case EDGES_BITSET: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    long bitsetOffset = movingOffset;
                    long bitsetSize = BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                    if (next < min) {
                        return takeFirstValueOnLeft(nodes.getLong(bitsetOffset + bitsetSize)) - 1;
                    } else if (next > max) {
                        return takeLastValueOnRight(nodeOffset);
                    }

                    int index = BufferBitSet.cardinalityTo(nodes, bitsetOffset, next - min);
                    movingOffset += bitsetSize;

                    if (!BufferBitSet.get(nodes, bitsetOffset, next - min)) {
                        return takeFirstValueOnLeft(nodes.getLong(movingOffset + index * Long.BYTES)) - 1;
                    }

                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
                    break;
                }
                case EDGES_CONDENSED: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));

                    if (next < min) {
                        return takeFirstValueOnLeft(nodes.getLong(movingOffset)) - 1;
                    } else if (next > max) {
                        return takeLastValueOnRight(nodeOffset);
                    }

                    int index = next - min;
                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
                    break;
                }
                case EDGES_NONE:
                    return maybeValue;
            }
        }
    }
}