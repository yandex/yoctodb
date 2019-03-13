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
 * Trie
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
     * We unable to provide getLongUnsafe() implementation since
     * there is no continuous buffer with key value.
     *
     * @param i index of key
     * @throws UnsupportedOperationException
     */
    @Override
    public long getLongUnsafe(int i) {
        throw new UnsupportedOperationException();
    }

    /**
     * We unable to provide getIntUnsafe() implementation since
     * there is no continuous buffer with key value.
     *
     * @param i index of key
     * @throws UnsupportedOperationException
     */
    @Override
    public int getIntUnsafe(int i) {
        throw new UnsupportedOperationException();
    }

    /**
     * We unable to provide getShortUnsafe() implementation since
     * there is no continuous buffer with key value.
     *
     * @param i index of key
     * @throws UnsupportedOperationException
     */
    @Override
    public short getShortUnsafe(int i) {
        throw new UnsupportedOperationException();
    }

    /**
     * We unable to provide getCharUnsafe() implementation since
     * there is no continuous buffer with key value.
     *
     * @param i index of key
     * @throws UnsupportedOperationException
     */
    @Override
    public char getCharUnsafe(int i) {
        throw new UnsupportedOperationException();
    }

    /**
     * We unable to provide getByteUnsafe() implementation since
     * there is no continuous buffer with key value.
     *
     * @param i index of key
     * @throws UnsupportedOperationException
     */
    @Override
    public byte getByteUnsafe(int i) {
        throw new UnsupportedOperationException();
    }

    /**
     * Look up for key in trie.
     * Complexity: O(|key|)
     *
     * @param element the element to lookup
     *
     * @return index of key matching the query or -1 if there is no match
     */
    @Override
    public int indexOf(@NotNull Buffer element) {
        if (size() > 0) {
            return indexOf(new BufferIterator(element));
        }

        return NOT_FOUND;
    }

    /**
     * Looks up for index of key, that would be greater than element with respect to orEquals
     * Complexity: O(|key|)
     *
     * @param element            element to compare to
     * @param orEquals           inclusive flag
     * @param upToIndexInclusive right bound (inclusive)
     *
     * @return index of key matching the query or -1 if there is no match
     */
    @Override
    public int indexOfGreaterThan(@NotNull Buffer element, boolean orEquals, int upToIndexInclusive) {
        if (size() > 0) {
            int result = indexOfGreaterThan(new BufferIterator(element), orEquals);
            return result == keysCount ? NOT_FOUND : Math.min(result, upToIndexInclusive);
        }

        return NOT_FOUND;
    }

    /**
     * Looks up for index of key, that would be less than element with respect to orEquals
     * Complexity: O(|key|)
     *
     * @param element            element to compare to
     * @param orEquals           inclusive flag
     * @param fromIndexInclusive left bound (inclusive)
     *
     * @return index of key matching the query or -1 if there is no match
     */
    @Override
    public int indexOfLessThan(@NotNull Buffer element, boolean orEquals, int fromIndexInclusive) {
        if (size() > 0) {
            return indexOfLessThan(new BufferIterator(element), orEquals);
        }

        return NOT_FOUND;
    }

    private int indexOf(@NotNull final BufferIterator key) {
        assert keysCount > 0;

        // start with the root node
        long movingOffset = 0L;

        // infinite loop for tail recursion emulation
        while (true) {
            // read metadata from the first byte of node
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if (hasPrefix(metadata)) { // there is a prefix, key must consume it to proceed
                final int prefixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                final BufferIterator prefix = new BufferIterator(nodes, movingOffset, prefixSize);
                if (key.compareToPrefix(prefix) != 0) {
                    // key didn't math the prefix
                    return NOT_FOUND;
                }
                movingOffset += prefixSize * Byte.BYTES;
            }

            int maybeValue = NOT_FOUND;
            if (hasValue(metadata)) { // there is a value, we must consume and store it before proceeding
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!key.hasNext()) {
                // key exhausted, return stored value (or NOT_FOUND if this not didn't have value)
                return maybeValue;
            }

            // we still have bytes in a key - let's find moves
            final int next = key.next();
            switch (edgeType(metadata)) {
                case EDGES_SINGLE: {
                    // there is only one path to the next node
                    if (next != Byte.toUnsignedInt(nodes.get(movingOffset++))) {
                        // path didn't match key, there is no node we are looking for
                        return NOT_FOUND;
                    }
                    movingOffset = nodes.getLong(movingOffset); // jump to the next node
                    break;
                }
                case EDGES_BITSET: {
                    // there is a bitset backed map for passing this node
                    // for compression and performance reasons we stored min and max value for the transition
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    if (min > next || max < next || !BufferBitSet.get(nodes, movingOffset, next - min)) {
                        // path didn't match key, there is no node we are looking for
                        return NOT_FOUND;
                    }
                    // count bits before desired index, that would be index in transitions array
                    int index = BufferBitSet.cardinalityTo(nodes, movingOffset, next - min);
                    movingOffset += BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES); // jump to the next node
                    break;
                }
                case EDGES_CONDENSED: {
                    // there is a continuous range of edges [min, max]
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    if (min > next || max < next) {
                        // path didn't match key, there is no node we are looking for
                        return NOT_FOUND;
                    }
                    int index = next - min;
                    movingOffset = nodes.getLong(movingOffset + index * Long.BYTES); // jump to the next node
                    break;
                }
                default:
                    // we reached terminal node and key still have bytes that we didn't consume. Key not found
                    return NOT_FOUND;
            }
        }
    }

    private int takeFirstValueOnLeft(long movingOffset) {
        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if (hasPrefix(metadata)) { // there is a prefix
                int prefixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                movingOffset += prefixSize * Byte.BYTES;
            }

            if (hasValue(metadata)) { // there is a value
                return nodes.getInt(movingOffset);
            }

            switch (edgeType(metadata)) {
                case EDGES_BITSET: {
                    int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    int max = Byte.toUnsignedInt(nodes.get(movingOffset++));

                    movingOffset += BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                }
                case EDGES_CONDENSED: {
                    movingOffset += 2 * Byte.BYTES;
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                }
                default:
                    /**
                     * This section must be unreachable since we cannot have a correct trie node with
                     * no value and edges different than EDGES_BITSET or EDGES_CONDENSED.
                     */
            }
        }
    }

    private int takeLastValueOnRight(long movingOffset) {
        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if (hasPrefix(metadata)) { // there is a prefix
                int prefixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                movingOffset += prefixSize * Byte.BYTES;
            }

            int value = NOT_FOUND;
            if (hasValue(metadata)) { // there is a value
                value = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            switch (edgeType(metadata)) {
                case EDGES_SINGLE: {
                    movingOffset += Byte.BYTES;
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                }
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
                default:
                    /**
                     * This section must be unreachable since we cannot have a correct trie node with
                     * no value and edges different than EDGES_BITSET or EDGES_CONDENSED.
                     */
            }
        }
    }

    private int indexOfGreaterThan(@NotNull final BufferIterator key, final boolean orEquals) {
        assert keysCount > 0;

        // start with the root node
        long movingOffset = 0L;
        // fix every node start offset
        long nodeOffset;

        while (true) {
            // fix node start
            nodeOffset = movingOffset;
            // read metadata from the first byte of node
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if (hasPrefix(metadata)) { // there is a prefix, key must consume it to proceed
                final int prefixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                final BufferIterator prefix = new BufferIterator(nodes, movingOffset, prefixSize);
                // compare key to prefix lexicographically
                final int prefixCompare = key.compareToPrefix(prefix);
                if (prefixCompare > 0) { // key > node, answer would be the last value of this node + 1
                    return takeLastValueOnRight(nodeOffset) + 1;
                } else if (prefixCompare < 0) { // prefix > key, answer would be first value of this node
                    return takeFirstValueOnLeft(nodeOffset);
                }
                movingOffset += prefixSize * Byte.BYTES;
            }

            int maybeValue = NOT_FOUND;
            if (hasValue(metadata)) { // there is a value, we must consume and store it before proceeding
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!key.hasNext()) {
                // key exhausted, return stored value (or first value under this node if it does not have any)
                if (maybeValue != NOT_FOUND) {
                    return orEquals ? maybeValue : maybeValue + 1;
                }
                return takeFirstValueOnLeft(nodeOffset);
            }

            // we still have bytes in a key - let's find moves
            final int next = key.next();
            switch (edgeType(metadata)) {
                case EDGES_SINGLE: {
                    // there is only one path to the next node
                    int path = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    if (next > path) {
                        return takeLastValueOnRight(nodes.getLong(movingOffset)) + 1;
                    } else if (next < path) {
                        return takeFirstValueOnLeft(nodes.getLong(movingOffset));
                    }
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                }
                case EDGES_BITSET: {
                    // there is a bitset backed map for passing this node
                    // for compression and performance reasons we stored min and max value for the transition
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
                    // there is a continuous range of edges [min, max]
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

    private int indexOfLessThan(@NotNull final BufferIterator key, final boolean orEquals) {
        assert keysCount > 0;

        long movingOffset = 0L;
        long nodeOffset;

        while (true) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            nodeOffset = movingOffset;
            movingOffset += Byte.BYTES;

            if (hasPrefix(metadata)) { // there is a prefix, key must consume it to proceed
                final int prefixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                final BufferIterator prefix = new BufferIterator(nodes, movingOffset, prefixSize);
                // compare key to prefix lexicographically
                final int prefixCompare = key.compareToPrefix(prefix);
                if (prefixCompare > 0) { // key > node, answer would be the last value of this node
                    return takeLastValueOnRight(nodeOffset);
                } else if (prefixCompare < 0) { // prefix > key, answer would be the first value of this node - 1
                    return takeFirstValueOnLeft(nodeOffset) - 1;
                }
                movingOffset += prefixSize * Byte.BYTES;
            }

            int maybeValue = NOT_FOUND;
            if (hasValue(metadata)) { // there is a value, we must consume and store it before proceeding
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!key.hasNext()) {
                // key exhausted, return stored value (or first value under this node - 1 if it does not have any)
                if (maybeValue != NOT_FOUND) {
                    return orEquals ? maybeValue : maybeValue - 1;
                }
                return takeFirstValueOnLeft(nodeOffset) - 1;
            }

            // we still have bytes in a key - let's find moves
            final int next = key.next();
            switch (edgeType(metadata)) {
                case EDGES_SINGLE: {
                    // there is only one path to the next node
                    int path = Byte.toUnsignedInt(nodes.get(movingOffset++));
                    if (next > path) {
                        return takeLastValueOnRight(nodes.getLong(movingOffset));
                    } else if (next < path) {
                        return takeFirstValueOnLeft(nodes.getLong(movingOffset)) - 1;
                    }
                    movingOffset = nodes.getLong(movingOffset);
                    break;
                }
                case EDGES_BITSET: {
                    // there is a bitset backed map for passing this node
                    // for compression and performance reasons we stored min and max value for the transition
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
                    // there is a continuous range of edges [min, max]
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
