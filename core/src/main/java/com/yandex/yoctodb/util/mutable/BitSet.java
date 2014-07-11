/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.util.mutable;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

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
     * Modify current bit set by applying bitwise {@code &} operation
     *
     * @param set source bit set
     *
     * @return whether there are nonzero bits
     */
    boolean and(
            @NotNull
            BitSet set);

    /**
     * Modify current bit set by applying bitwise {@code &} operation
     *
     * @param setInByteBuffer source bit set
     *
     * @return whether there are nonzero bits
     */
    boolean or(
            @NotNull
            ByteBuffer setInByteBuffer,
            int startPosition,
            int bitSetSizeInLongs);

    /**
     * Checks whether there are nonzero bits in current bit set
     *
     * @return whether there are nonzero bits in current bit set
     */
    boolean isEmpty();
}
