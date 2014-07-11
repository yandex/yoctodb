/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.mutable.util;

import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author svyatoslav
 *         Date: 25.11.13
 */
public class LongArrayBitSetTest {
    private static final int SIZE = 1024;

    @Test
    public void simpleAndTest() {
        for (int i = 1; i < SIZE; i++) {
            final BitSet bs1 = LongArrayBitSet.one(i);
            final BitSet bs2 = LongArrayBitSet.one(i);
            Assert.assertEquals(i, bs1.cardinality());
            bs1.and(bs2);
            Assert.assertEquals(i, bs1.cardinality());
        }
    }

    @Test
    public void simpleOrTestWithByteBufferBitSet() throws IOException {
        for (int i = 1; i < SIZE; i++) {
            final BitSet bs1 = LongArrayBitSet.one(i);
            final LongArrayBitSet bs2 = (LongArrayBitSet) LongArrayBitSet.one(i);

            Assert.assertEquals(i, bs1.cardinality());
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            for (long word : bs2.toArray()) {
                os.write(Longs.toByteArray(word));
            }
            ByteBuffer bs2BB = ByteBuffer.wrap(os.toByteArray());

            bs1.or(bs2BB, 0, bs2.toArray().length);
            Assert.assertEquals(i, bs1.cardinality());
        }
    }

    @Test
    public void clearTest() {
        for (int size = 1; size <= SIZE; size++) {
            final BitSet bitSet = LongArrayBitSet.one(size);
            Assert.assertEquals(size, bitSet.cardinality());
            bitSet.clear();
            Assert.assertEquals(0, bitSet.cardinality());
        }
    }

    @Test
    public void interleavingIteratorTest() {
        final BitSet s = LongArrayBitSet.zero(SIZE);
        for (int i = 0; i < s.getSize(); i += 2)
            s.set(i);
        for (int i = 0; i < s.getSize(); i += 2) {
            Assert.assertEquals(i, s.nextSetBit(i));
        }
    }

    @Test
    public void zerosIteratorTest() {
        final BitSet s = LongArrayBitSet.zero(SIZE);
        Assert.assertEquals(-1, s.nextSetBit(0));
    }

    @Test
    public void onesIteratorTest() {
        final BitSet s = LongArrayBitSet.one(SIZE);
        for (int i = 0; i < s.getSize(); i ++) {
            Assert.assertEquals(i, s.nextSetBit(i));
        }
    }

}
