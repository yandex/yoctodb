/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.mutable.util;

import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author svyatoslav
 */
public class LongArrayBitSetTest {
    private static final int SIZE = 1024;

    @Test
    public void inverse() {
        for (int i = 1; i < SIZE; i++) {
            final BitSet bs = LongArrayBitSet.one(i);
            assertFalse(bs.inverse());
            assertTrue(bs.inverse());
            assertEquals(i, bs.cardinality());
        }
    }

    @Test
    public void or() {
        for (int i = 1; i < SIZE; i++) {
            final BitSet bs1 = LongArrayBitSet.one(i);
            final BitSet bs2 = LongArrayBitSet.zero(i);
            assertEquals(i, bs1.cardinality());
            assertEquals(0, bs2.cardinality());
            bs2.or(bs1);
            assertEquals(i, bs2.cardinality());
        }
    }

    @Test
    public void empty() {
        for (int i = 1; i < SIZE; i++) {
            assertTrue(LongArrayBitSet.zero(i).isEmpty());
        }
    }

    @Test
    public void simpleAndTest() {
        for (int i = 1; i < SIZE; i++) {
            final BitSet bs1 = LongArrayBitSet.one(i);
            final BitSet bs2 = LongArrayBitSet.one(i);
            assertEquals(i, bs1.cardinality());
            bs1.and(bs2);
            assertEquals(i, bs1.cardinality());
        }
    }

    @Test
    public void simpleOrTestWithByteBufferBitSet() throws IOException {
        for (int i = 1; i < SIZE; i++) {
            final BitSet bs1 = LongArrayBitSet.one(i);
            final LongArrayBitSet bs2 = (LongArrayBitSet) LongArrayBitSet.one(i);

            assertEquals(i, bs1.cardinality());
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            for (long word : bs2.toArray()) {
                os.write(Longs.toByteArray(word));
            }
            Buffer bs2BB = Buffer.from(os.toByteArray());

            bs1.or(bs2BB, 0, bs2.toArray().length);
            assertEquals(i, bs1.cardinality());
        }
    }

    @Test
    public void clearTest() {
        for (int size = 1; size <= SIZE; size++) {
            final BitSet bitSet = LongArrayBitSet.one(size);
            assertEquals(size, bitSet.cardinality());
            bitSet.clear();
            assertEquals(0, bitSet.cardinality());
        }
    }

    @Test
    public void interleavingIteratorTest() {
        final BitSet s = LongArrayBitSet.zero(SIZE);
        for (int i = 0; i < s.getSize(); i += 2)
            s.set(i);
        for (int i = 0; i < s.getSize(); i += 2) {
            assertEquals(i, s.nextSetBit(i));
        }
    }

    @Test
    public void zerosIteratorTest() {
        final BitSet s = LongArrayBitSet.zero(SIZE);
        assertEquals(-1, s.nextSetBit(0));
    }

    @Test
    public void onesIteratorTest() {
        final BitSet s = LongArrayBitSet.one(SIZE);
        for (int i = 0; i < s.getSize(); i ++) {
            assertEquals(i, s.nextSetBit(i));
        }
    }

}
