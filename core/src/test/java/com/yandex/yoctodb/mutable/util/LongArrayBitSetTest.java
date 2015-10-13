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
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author svyatoslav
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
            Buffer bs2BB = Buffer.from(os.toByteArray());

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
