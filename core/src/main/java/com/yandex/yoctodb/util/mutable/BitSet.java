/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

/**
 * Immutable bit set
 *
 * @author incubos
 */
public interface BitSet {
    /**
     * Bit set size (number of stored bits)
     *
     * @return number of stored bits
     */
    int getSize();

    /**
     * Calculates number of ones
     *
     * @return number of ones
     */
    int cardinality();

    /**
     * Set {@code i}th bit to {@code 1}
     *
     * @param i bit
     */
    void set(int i);

    /**
     * Clear all bits
     */
    void clear();

    /**
     * Inverse all bits
     *
     * @return whether there are nonzero bits
     */
    boolean inverse();

    /**
     * Set all bits
     */
    void set();

    /**
     * Gets {@code i}th bit value
     *
     * @param i bit
     *
     * @return {@code i}th bit value
     */
    boolean get(int i);

    /**
     * Index of next set bit or -1
     *
     * @param fromIndexInclusive start index inclusive
     * @return index of next set bit or -1
     */
    int nextSetBit(int fromIndexInclusive);

    /**
     * Modify current bit set by applying bitwise {@code AND} operation
     *
     * @param set source bit set
     *
     * @return whether there are nonzero bits
     */
    boolean and(
            @NotNull
            BitSet set);

    /**
     * Modify current bit set by applying bitwise {@code OR}
     *
     * @param set source bit set
     *
     * @return whether there are nonzero bits
     */
    boolean or(
            @NotNull
            BitSet set);

    /**
     * Modify current bit set by applying bitwise {@code &} operation
     *
     * @param longArrayBitSetInByteBuffer source bit set
     * @param startPosition               position to start reading from
     * @param bitSetSizeInLongs           bit set size in {@code long}s
     *
     * @return whether there are nonzero bits
     */
    boolean or(
            @NotNull
            Buffer longArrayBitSetInByteBuffer,
            int startPosition,
            int bitSetSizeInLongs);

    /**
     * Checks whether there are nonzero bits in current bit set
     *
     * @return whether there are nonzero bits in current bit set
     */
    boolean isEmpty();
}
