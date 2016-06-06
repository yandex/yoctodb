/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.mutable.BitSet;

/**
 * Read-only zero {@link BitSet} implementation
 *
 * @author incubos
 */
@ThreadSafe
public final class ReadOnlyZeroBitSet implements BitSet {
    private final int size;

    public ReadOnlyZeroBitSet(final int size) {
        this.size = size;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public int cardinality() {
        return 0;
    }

    @Override
    public void set(final int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean get(final int i) {
        assert 0 <= i && i < size;

        return false;
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean inverse() {
        throw new UnsupportedOperationException();
    }

    public void set() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean and(
            @NotNull
            final BitSet set) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean or(
            @NotNull
            final BitSet set) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    /**
     * See {@link java.util.BitSet#nextSetBit(int)}
     */
    @Override
    public int nextSetBit(final int fromIndexInclusive) {
        assert fromIndexInclusive >= 0;

        return -1;
    }
}
