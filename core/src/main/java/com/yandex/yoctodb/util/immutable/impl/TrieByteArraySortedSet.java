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
import com.yandex.yoctodb.util.common.BufferIterator;
import com.yandex.yoctodb.util.immutable.ByteArraySortedSet;
import org.jetbrains.annotations.NotNull;

import static com.yandex.yoctodb.util.common.TrieNodeMetadata.*;

/**
 * Trie-based implementation for keys storage.
 * Provides an average complexity O(|key|) for all operations with
 * worst-case performance proportional to the depth of compressed trie.
 *
 * @author Andrey Korzinev (ya-goodfella@yandex.com)
 */
public class TrieByteArraySortedSet implements ByteArraySortedSet {
    private static final int NOT_FOUND = -1;

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

    /**
     * @return keys count in this index.
     */
    @Override
    public int size() {
        return keysCount;
    }

    /**
     * We unable to provide get() implementation since
     * there is no continuous buffer with key value.
     *
     * @param i index of key
     * @throws UnsupportedOperationException
     */
    @NotNull
    @Override
    public Buffer get(int i) {
        throw new UnsupportedOperationException();
    }

    /**
     * Look up for key in trie.
     * Complexity: O(|key|)
     *
     * @param e the element to lookup
     *
     * @return index of key matching the query or -1 if there is no match
     */
    @Override
    public int indexOf(@NotNull Buffer e) {
        if (size() > 0) {
            return indexOf(new BufferIterator(e));
        }

        return NOT_FOUND;
    }

    /**
     * Looks up for index of key, that would be greater than element with respect to orEquals
     * Complexity: O(|key|)
     *
     * @param e                  element to compare to
     * @param orEquals           inclusive flag
     * @param upToIndexInclusive right bound (inclusive)
     *
     * @return index of key matching the query or -1 if there is no match
     */
    @Override
    public int indexOfGreaterThan(@NotNull Buffer e, boolean orEquals, int upToIndexInclusive) {
        if (size() > 0) {
            int result = indexOfGreaterThan(new BufferIterator(e), orEquals);
            return result == keysCount ? NOT_FOUND : Math.min(result, upToIndexInclusive);
        }

        return NOT_FOUND;
    }

    /**
     * Looks up for index of key, that would be less than element with respect to orEquals
     * Complexity: O(|key|)
     *
     * @param e                  element to compare to
     * @param orEquals           inclusive flag
     * @param fromIndexInclusive left bound (inclusive)
     *
     * @return index of key matching the query or -1 if there is no match
     */
    @Override
    public int indexOfLessThan(@NotNull Buffer e, boolean orEquals, int fromIndexInclusive) {
        if (size() > 0) {
            return indexOfLessThan(new BufferIterator(e), orEquals);
        }

        return NOT_FOUND;
    }

    private int indexOf(@NotNull final BufferIterator query) {
        assert keysCount > 0;

        long movingOffset = 0L;

        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if (hasInfix(metadata)) { // there is an infix
                final int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                final BufferIterator infix = new BufferIterator(nodes, movingOffset, infixSize);
                if (BufferIterator.strip(query, infix) != 0) {
                    return NOT_FOUND;
                }
                movingOffset += infixSize * Byte.BYTES;
            }

            int maybeValue = NOT_FOUND;
            if (hasValue(metadata)) { // there is a value
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!query.hasNext()) {
                return maybeValue;
            }

            final int next = query.next();
            switch (edgeType(metadata)) {
                case EDGES_SINGLE:
                    if (next != Byte.toUnsignedInt(nodes.get(movingOffset++))) {
                        return NOT_FOUND;
                    }
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                case EDGES_BITSET: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    if (min > next || max < next || !BufferBitSet.get(nodes, movingOffset, next - min)) {
                        return NOT_FOUND;
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
                        return NOT_FOUND;
                    }
                    int index = next - min;
                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
                    break;
                }
                default:
                    return NOT_FOUND;
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

            int value = NOT_FOUND;
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
                default: // should be unreachable
            }
        }
    }

    private int indexOfGreaterThan(@NotNull final BufferIterator query, final boolean orEquals) {
        assert keysCount > 0;

        long movingOffset = 0L;
        long nodeOffset;

        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            nodeOffset = movingOffset;
            movingOffset += Byte.BYTES;

            if (hasInfix(metadata)) { // there is an infix
                final int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                final BufferIterator infix = new BufferIterator(nodes, movingOffset, infixSize);
                final int infixCompare = BufferIterator.strip(query, infix);
                if (infixCompare > 0) { // query > node
                    return takeLastValueOnRight(nodeOffset) + 1;
                } else if (infixCompare < 0) { // infix > query
                    return takeFirstValueOnLeft(nodeOffset);
                }
                movingOffset += infixSize * Byte.BYTES;
            }

            int maybeValue = NOT_FOUND;
            if (hasValue(metadata)) { // there is a value
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!query.hasNext()) {
                if (maybeValue != NOT_FOUND) {
                    return orEquals ? maybeValue : maybeValue + 1;
                }
                return takeFirstValueOnLeft(nodeOffset);
            }

            final int next = query.next();
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

    private int indexOfLessThan(@NotNull final BufferIterator query, final boolean orEquals) {
        assert keysCount > 0;

        long movingOffset = 0L;
        long nodeOffset;

        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            nodeOffset = movingOffset;
            movingOffset += Byte.BYTES;

            if (hasInfix(metadata)) { // there is an infix
                final int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                final BufferIterator infix = new BufferIterator(nodes, movingOffset, infixSize);
                final int infixCompare = BufferIterator.strip(query, infix);
                if (infixCompare > 0) { // query > node
                    return takeLastValueOnRight(nodeOffset);
                } else if (infixCompare < 0) { // infix > query
                    return takeFirstValueOnLeft(nodeOffset) - 1;
                }
                movingOffset += infixSize * Byte.BYTES;
            }

            int maybeValue = NOT_FOUND;
            if (hasValue(metadata)) { // there is a value
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!query.hasNext()) {
                if (maybeValue != NOT_FOUND) {
                    return orEquals ? maybeValue : maybeValue - 1;
                }
                return takeFirstValueOnLeft(nodeOffset) - 1;
            }

            final int next = query.next();
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
