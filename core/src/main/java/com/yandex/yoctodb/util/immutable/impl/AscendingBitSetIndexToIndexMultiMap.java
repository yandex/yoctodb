package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
 * {@link IndexToIndexMultiMap} implementation based on accumulated {@link BitSet}s.
 * Main idea of this index is storing not the documents that satisfies key,
 * but only the ones which less than that key.
 *
 * Restriction: document with this index type must not provide more than one value.
 *
 * @author Andrey Korzinev (goodfella@yandex-team.ru)
 */

@Immutable
public class AscendingBitSetIndexToIndexMultiMap implements IndexToIndexMultiMap {

    private final int keysCount;
    @NotNull
    private final Buffer elements;
    private BitSet nonNullBitSet; // lazy-evaluated
    private final int bitSetSizeInLongs;
    private final long bitSetSizeInBytes;

    @NotNull
    public static IndexToIndexMultiMap from(
            @NotNull
            final Buffer buf) {
        final int keysCount = buf.getInt();
        final int bitSetSizeInLongs = buf.getInt();
        final Buffer elements = buf.slice();

        return new AscendingBitSetIndexToIndexMultiMap(
                keysCount,
                elements.slice(),
                bitSetSizeInLongs);
    }

    private AscendingBitSetIndexToIndexMultiMap(
            final int keysCount,
            @NotNull
            final Buffer elements,
            final int bitSetSizeInLongs) {
        assert keysCount >= 0 : "Negative keys count";
        this.keysCount = keysCount;
        this.bitSetSizeInLongs = bitSetSizeInLongs;
        this.bitSetSizeInBytes = ((long) bitSetSizeInLongs) << 3;
        this.elements = elements;
    }

    /**
     * @param sizeHint hint for bit set size
     * @return {@link BitSet} that contains every document that was associated with key (e.g. not null)
     */
    private BitSet getNonNull(int sizeHint) {
        if (nonNullBitSet == null) {
            nonNullBitSet = LongArrayBitSet.zero(sizeHint);
            nonNullBitSet.or(elements, keysCount * bitSetSizeInBytes, bitSetSizeInLongs);
        }

        assert nonNullBitSet.getSize() == sizeHint;

        return nonNullBitSet;
    }

    /**
     * Sets document bits for provided key index
     *
     * Complexity: O(1)
     * Additional space: O(1)
     *
     * @param dest destination {@link BitSet}
     * @param key key index
     * @return true if destination have non-zero bits, false otherwise
     */
    @Override
    public boolean get(
            @NotNull
            final BitSet dest,
            final int key) {
        assert 0 <= key && key < keysCount;

        final long start = key * bitSetSizeInBytes;

        ArrayBitSet target = LongArrayBitSet.zero(dest.getSize());

        target.or(elements, start + bitSetSizeInBytes, bitSetSizeInLongs);
        target.xor(elements, start, bitSetSizeInLongs);

        return dest.or(target);
    }

    /**
     * Sets document bits for documents for keys which is greater or equals to key index
     *
     * Complexity: O(1)
     * Additional space: O(1)
     *
     * @param dest destination {@link BitSet}
     * @param fromInclusive lowest key index
     * @return true if destination have non-zero bits, false otherwise
     */
    @Override
    public boolean getFrom(
            @NotNull
            final BitSet dest,
            final int fromInclusive) {
        assert 0 <= fromInclusive && fromInclusive < keysCount;

        ArrayBitSet target = LongArrayBitSet.zero(dest.getSize());
        target.or(getNonNull(dest.getSize()));
        target.xor(elements, fromInclusive * bitSetSizeInBytes, bitSetSizeInLongs);

        return dest.or(target);
    }



    /**
     * Sets document bits for documents for keys which is less than key index
     *
     * Complexity: O(1)
     * Additional space: -
     *
     * @param dest destination {@link BitSet}
     * @param toExclusive highest key index
     * @return true if destination have non-zero bits, false otherwise
     */
    @Override
    public boolean getTo(
            @NotNull
            final BitSet dest,
            final int toExclusive) {
        assert 0 < toExclusive && toExclusive <= keysCount;

        // edge case optimization
        if (toExclusive == keysCount) {
            return dest.or(getNonNull(dest.getSize()));
        }

        return dest.or(elements, toExclusive * bitSetSizeInBytes, bitSetSizeInLongs);
    }

    /**
     * Sets document bits for documents for keys which is less than key index
     *
     * Complexity: O(1)
     * Additional space: O(1)
     *
     * @param dest destination {@link BitSet}
     * @param fromInclusive
     * @param toExclusive
     * @return
     */
    @Override
    public boolean getBetween(
            @NotNull
            final BitSet dest,
            final int fromInclusive,
            final int toExclusive) {
        assert 0 <= fromInclusive &&
                fromInclusive < toExclusive &&
                toExclusive <= keysCount;

        ArrayBitSet target = LongArrayBitSet.zero(dest.getSize());

        // edge case optimization
        if (toExclusive == keysCount) {
            target.or(getNonNull(dest.getSize()));
        } else {
            target.or(elements, toExclusive * bitSetSizeInBytes, bitSetSizeInLongs);
        }

        target.xor(elements, fromInclusive * bitSetSizeInBytes, bitSetSizeInLongs);

        return dest.or(target);
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
