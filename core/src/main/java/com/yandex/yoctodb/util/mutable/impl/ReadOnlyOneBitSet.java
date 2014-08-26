/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.mutable.BitSet;

import java.nio.ByteBuffer;

/**
 * Read-only one {@link BitSet} implementation
 *
 * @author incubos
 */
@ThreadSafe
public final class ReadOnlyOneBitSet implements BitSet {
    private final int size;

    public ReadOnlyOneBitSet(final int size) {
        assert size > 0;

        this.size = size;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public int cardinality() {
        return size;
    }

    @Override
    public void set(final int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean get(final int i) {
        assert 0 <= i && i < size;

        return true;
    }

    public void clear() {
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
            final ByteBuffer longArrayBitSetInByteBuffer,
            final int startPosition,
            final int bitSetSizeInLongs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * See {@link java.util.BitSet#nextSetBit(int)}
     */
    @Override
    public int nextSetBit(final int fromIndexInclusive) {
        assert fromIndexInclusive >= 0;
        if (fromIndexInclusive >= size)
            return -1;
        else
            return fromIndexInclusive;
    }
}
