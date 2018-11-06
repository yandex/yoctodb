package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.common.BufferIterator;
import com.yandex.yoctodb.util.immutable.ByteArraySortedSet;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class TrieByteArraySortedSet implements ByteArraySortedSet {
    private final int keysCount;
    private final Buffer nodes;

    public static TrieByteArraySortedSet from(
            @NotNull
            final Buffer buffer) {
        final int size = buffer.getInt();
        final Buffer nodes = buffer.slice();

        return new TrieByteArraySortedSet(
                nodes,
                size);
    }

    public TrieByteArraySortedSet(@NotNull Buffer nodes,
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
        return null;
    }

    @Override
    public int indexOf(@NotNull Buffer e) {
        final Iterator<Byte> bytes = new BufferIterator(e);
        return indexOf(bytes);
    }

    /**
     *
     * @param e
     * @return
     */
    private int indexOf(@NotNull final Iterator<Byte> e) {
        long movingOffset = 0L;
        int value = -1;

        while (movingOffset >= 0) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            boolean infixStripped = true;
            if ((metadata & 0b0001) != 0) { // there is an infix
                int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                Iterator<Byte> infix = new BufferIterator(nodes, movingOffset, infixSize);
                movingOffset += infixSize * Byte.BYTES;
                int infixCompare = BufferIterator.compareMutableIterators(e, infix);
                infixStripped = !infix.hasNext() && infixCompare >= 0;
            }

            if (!infixStripped) break;

            int maybeValue = -1;
            if ((metadata & 0b0010) != 0) { // there is a value
                maybeValue = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!e.hasNext()) {
                value = maybeValue;
                break;
            }

            int next = Byte.toUnsignedInt(e.next());
            int edgesMeta = metadata & 0b1100;
            if (edgesMeta == 0b0100) { // single edge
                int key = Byte.toUnsignedInt(nodes.get(movingOffset));
                if (key != next) break;

                movingOffset += Byte.BYTES;
                movingOffset = nodes.getLong(movingOffset);
            } else if (edgesMeta == 0b1100) { // condensed edges
                int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                if (min > next || max < next) break;

                int index = next - min;
                movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
            } else if (edgesMeta == 0b1000) {
                int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                int max = Byte.toUnsignedInt(nodes.get(movingOffset++));

                if (min > next || max < next || !BufferBitSet.get(nodes, movingOffset, next - min)) break;

                int index = BufferBitSet.cardinalityTo(nodes, movingOffset, next - min);
                movingOffset += BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
            } else if (e.hasNext()) { // we reached node without edges and still have elements in query
                return -1;
            }
        }

        return value;
    }

    private int takeFirstValueOnLeft(long movingOffset) {
        while (movingOffset >= 0) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if ((metadata & 0b0001) != 0) { // there is an infix
                int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                movingOffset += infixSize * Byte.BYTES;
            }

            if ((metadata & 0b0010) != 0) { // there is a value
                return nodes.getInt(movingOffset);
            }

            int edgesMeta = metadata & 0b1100;
            if (edgesMeta == 0b0100) {
                movingOffset += Byte.BYTES;
                movingOffset = nodes.getLong(movingOffset);
            } else if (edgesMeta == 0b1100) { // condensed edges
                movingOffset += 2;
                movingOffset = nodes.getLong(movingOffset);
            } else if (edgesMeta == 0b1000) {
                int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                movingOffset += BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                movingOffset = nodes.getLong(movingOffset);
            }
        }

        return -1;
    }

    private int takeLastValueOnRight(long movingOffset) {
        int value = -1;
        while (movingOffset >= 0) {
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            if ((metadata & 0b0001) != 0) { // there is an infix
                int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                movingOffset += infixSize * Byte.BYTES;
            }

            if ((metadata & 0b0010) != 0) { // there is a value
                value = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            int edgesMeta = metadata & 0b1100;
            if (edgesMeta == 0b0100) {
                movingOffset += Byte.BYTES;
                movingOffset = nodes.getLong(movingOffset);
            } else if (edgesMeta == 0b1100) { // condensed edges
                int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                movingOffset = nodes.getLong(movingOffset + (max - min) * Long.BYTES);
            } else if (edgesMeta == 0b1000) {
                int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                int index = BufferBitSet.cardinalityTo(nodes, movingOffset, max - min);
                movingOffset += BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
            } else {
                break;
            }
        }

        return value;
    }

    @Override
    public int indexOfGreaterThan(@NotNull Buffer e, boolean orEquals, int upToIndexInclusive) {
        int result = indexOfGreaterThan(new BufferIterator(e), orEquals);
        return result == keysCount ? -1 : result;
    }

    private int indexOfGreaterThan(@NotNull final Iterator<Byte> e, final boolean orEquals) {
        long movingOffset = 0L;
        long nodeOffset;
        int value = -1;

        while (movingOffset >= 0) {
            nodeOffset = movingOffset;
            final int metadata = Byte.toUnsignedInt(nodes.get(movingOffset));
            movingOffset += Byte.BYTES;

            int infixCompare = 0;
            boolean infixStripped = true;
            if ((metadata & 0b0001) != 0) { // there is an infix
                int infixSize = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
                Iterator<Byte> infix = new BufferIterator(nodes, movingOffset, infixSize);
                movingOffset += infixSize * Byte.BYTES;
                infixCompare = BufferIterator.compareMutableIterators(e, infix);
                infixStripped = !infix.hasNext() && infixCompare >= 0;
            }

            if (!infixStripped) {
                if (infixCompare > 0) {
                    return takeLastValueOnRight(nodeOffset) + 1;
                } else {
                    return takeFirstValueOnLeft(nodeOffset);
                }
            }

            if ((metadata & 0b0010) != 0) { // there is a value
                value = nodes.getInt(movingOffset);
                movingOffset += Integer.BYTES;
            }

            if (!e.hasNext()) {
                if (value != -1) {
                    return orEquals && (metadata & 0b0010) != 0 ? value : value + 1;
                } else {
                    return takeFirstValueOnLeft(nodeOffset);
                }
            }

            int next = Byte.toUnsignedInt(e.next());
            int edgesMeta = metadata & 0b1100;
            if (edgesMeta == 0b0100) { // single edge
                int key = Byte.toUnsignedInt(nodes.get(movingOffset));
                if (next > key) {
                    return takeLastValueOnRight(nodeOffset) + 1;
                } else if (next < key) {
                    return takeFirstValueOnLeft(nodeOffset);
                }

                movingOffset += Byte.BYTES;
                movingOffset = nodes.getLong(movingOffset);
            } else if (edgesMeta == 0b1100) { // condensed edges
                int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                int max = Byte.toUnsignedInt(nodes.get(movingOffset++));
                if (next > max) {
                    return takeLastValueOnRight(nodes.getLong(movingOffset + (max - min) * Long.BYTES)) + 1;
                } else if (next < min) {
                    return takeFirstValueOnLeft(nodes.getLong(movingOffset));
                }

                int index = next - min;
                movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
            } else if (edgesMeta == 0b1000) {
                int min = Byte.toUnsignedInt(nodes.get(movingOffset++));
                int max = Byte.toUnsignedInt(nodes.get(movingOffset++));

                if (next > max) {
                    return takeLastValueOnRight(
                            nodes.getLong(movingOffset +
                                    BufferBitSet.arraySize(max - min + 1) * Long.BYTES +
                                    BufferBitSet.cardinalityTo(nodes, movingOffset, max - min) * Long.BYTES)) + 1;
                } else if (next < min) {
                    return takeFirstValueOnLeft(nodes.getLong(movingOffset +
                            BufferBitSet.arraySize(max - min + 1) * Long.BYTES));
                } else if (!BufferBitSet.get(nodes, movingOffset, next - min)) {
                    int index = BufferBitSet.cardinalityTo(nodes, movingOffset, next - min) - 1;
                    return takeLastValueOnRight(
                            nodes.getLong(movingOffset +
                                    BufferBitSet.arraySize(max - min + 1) * Long.BYTES +
                                    index * Long.BYTES)) + 1;
                }

                int index = BufferBitSet.cardinalityTo(nodes, movingOffset, next - min);
                movingOffset += BufferBitSet.arraySize(max - min + 1) * Long.BYTES;
                movingOffset = nodes.getLong(movingOffset + index * Long.BYTES);
            } else if (e.hasNext()) {
                return value + 1;
            }
        }

        return value;
    }

    @Override
    public int indexOfLessThan(@NotNull Buffer e, boolean orEquals, int fromIndexInclusive) {
        return 0;
    }
}
