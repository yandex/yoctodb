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

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.BitSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ReadOnlyOneBitSet}
 *
 * @author incubos
 */
public class ReadOnlyOneBitSetTest {
    private final int SIZE = 1024;

    @Test
    public void get() {
        final BitSet bitSet = new ReadOnlyOneBitSet(SIZE);
        for (int i = 0; i < SIZE; i++)
            assertTrue(bitSet.get(i));
    }

    @Test
    public void size() {
        for (int i = 0; i < SIZE; i++)
            assertEquals(i, new ReadOnlyOneBitSet(i).getSize());
    }

    @Test
    public void cardinality() {
        for (int i = 0; i < SIZE; i++)
            assertEquals(i, new ReadOnlyOneBitSet(i).cardinality());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedPointSet() {
        new ReadOnlyOneBitSet(1).set(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedClear() {
        new ReadOnlyOneBitSet(1).clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedInverse() {
        new ReadOnlyOneBitSet(1).inverse();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedSet() {
        new ReadOnlyOneBitSet(1).set();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedAnd() {
        new ReadOnlyOneBitSet(1).and(LongArrayBitSet.one(1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedOr() {
        new ReadOnlyOneBitSet(1).or(LongArrayBitSet.zero(1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedXor() {
        new ReadOnlyOneBitSet(1).xor(LongArrayBitSet.zero(1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedAndBuffer() {
        new ReadOnlyOneBitSet(1).and(
                Buffer.from(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}),
                0,
                1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedXorBuffer() {
        new ReadOnlyOneBitSet(1).xor(
                Buffer.from(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}),
                0,
                1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedOrBuffer() {
        new ReadOnlyOneBitSet(1).or(
                Buffer.from(new byte[]{0, 0, 0, 0, 0, 0, 0, 0}),
                0,
                1);
    }

    @Test
    public void nextSetBit() {
        final BitSet bs = new ReadOnlyOneBitSet(SIZE);
        for (int i = 0; i < SIZE; i++) {
            assertEquals(i, bs.nextSetBit(i));
        }
        assertEquals(-1, bs.nextSetBit(SIZE));
    }
}
