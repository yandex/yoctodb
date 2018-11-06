/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.google.common.collect.TreeMultimap;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

public class AscendingBitSetIndexToIndexMultiMapTest {
    private static final int DOCS = 128;

    private static AscendingBitSetIndexToIndexMultiMap build() {
        final TreeMultimap<Integer, Integer> elements = TreeMultimap.create();

        for (int i = 0, j = 0; i < DOCS; ++i, j = i / 2) {
            elements.put(j, i);
        }

        final com.yandex.yoctodb.util.mutable.impl.AscendingBitSetIndexToIndexMultiMap mutable =
            new com.yandex.yoctodb.util.mutable.impl.AscendingBitSetIndexToIndexMultiMap(elements.asMap().values(), DOCS);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            mutable.writeTo(baos);
        } catch (IOException e) {
            e.printStackTrace();
        }

        final Buffer buf = Buffer.from(baos.toByteArray());

        assertEquals(
                V1DatabaseFormat.MultiMapType.ASCENDING_BIT_SET_BASED.getCode(),
                buf.getInt());

        final AscendingBitSetIndexToIndexMultiMap result =
                AscendingBitSetIndexToIndexMultiMap.from(buf);

        assertEquals(DOCS / 2, result.getKeysCount());

        return result;
    }

    @Test
    public void get() {
        AscendingBitSetIndexToIndexMultiMap index = build();

        final BitSet dest = LongArrayBitSet.zero(DOCS);
        final BitSet expected = LongArrayBitSet.zero(DOCS);
        expected.set(4);
        expected.set(5);

        assertTrue("Destination non-zero", index.get(dest, 2));
        dest.xor(expected);
        assertTrue("Documents 4 and 5 are set", dest.cardinality() == 0);
    }

    @Test
    public void getFrom() {
        AscendingBitSetIndexToIndexMultiMap index = build();

        final BitSet dest = LongArrayBitSet.zero(DOCS);
        final BitSet expected = LongArrayBitSet.zero(DOCS);
        expected.set(124);
        expected.set(125);
        expected.set(126);
        expected.set(127);

        assertTrue("Destination non-zero", index.getFrom(dest, 62));
        dest.xor(expected);
        assertTrue("Documents (124, 125, 126, 127) are set", dest.cardinality() == 0);
    }

    @Test
    public void getTo() {
        AscendingBitSetIndexToIndexMultiMap index = build();

        final BitSet dest = LongArrayBitSet.zero(DOCS);
        final BitSet expected = LongArrayBitSet.zero(DOCS);
        expected.set(0);
        expected.set(1);
        expected.set(2);
        expected.set(3);

        assertTrue("Destination non-zero", index.getTo(dest, 2));
        dest.xor(expected);
        assertTrue("Documents (0, 1, 2, 3) are set", dest.cardinality() == 0);
    }

    @Test
    public void getBetween() {
        AscendingBitSetIndexToIndexMultiMap index = build();

        final BitSet dest = LongArrayBitSet.zero(DOCS);
        final BitSet expected = LongArrayBitSet.zero(DOCS);
        expected.set(4);
        expected.set(5);
        expected.set(6);
        expected.set(7);

        assertTrue("Destination non-zero", index.getBetween(dest, 2, 4));
        dest.xor(expected);
        assertTrue("Documents (4, 5, 6, 7) are set", dest.cardinality() == 0);
    }

    @Test
    public void releaseToNullPool() {
        AscendingBitSetIndexToIndexMultiMap index = build();

        final BitSet dest = LongArrayBitSet.zero(DOCS);
        final BitSet expected = LongArrayBitSet.zero(DOCS);
        expected.set(4);
        expected.set(5);
        expected.set(6);
        expected.set(7);

        assertTrue("Destination non-zero", index.getBetween(dest, 2, 4));
        dest.xor(expected);
        assertTrue("Documents (4, 5, 6, 7) are set", dest.cardinality() == 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    @NotNull
    public void ascending() {
        build().ascending(LongArrayBitSet.one(DOCS));
    }

    @Test(expected = UnsupportedOperationException.class)
    @NotNull
    public void descending() {
        build().descending(LongArrayBitSet.one(DOCS));
    }

    @Test
    public void getKeysCount() {
        int keysCount = build().getKeysCount();
        assertEquals(DOCS / 2, keysCount);
    }

    @Test
    public void tostring() {
        assertNotNull(build().toString());
    }
}
