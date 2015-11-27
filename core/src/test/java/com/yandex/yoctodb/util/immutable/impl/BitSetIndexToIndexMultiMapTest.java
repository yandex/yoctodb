/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link BitSetIndexToIndexMultiMap}
 *
 * @author incubos
 */
public class BitSetIndexToIndexMultiMapTest {
    private final int DOCS = 128;

    private IndexToIndexMultiMap build() throws IOException {
        final com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap mutable =
                new com.yandex.yoctodb.util.mutable.impl.BitSetIndexToIndexMultiMap(DOCS);
        for (int i = 0; i < DOCS; i++) {
            mutable.put(i / 2, i);
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        assertEquals(
                V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode(),
                buf.getInt());

        final IndexToIndexMultiMap result =
                BitSetIndexToIndexMultiMap.from(buf);

        assertEquals(DOCS / 2, result.getKeysCount());

        return result;
    }

    @Test
    public void buildEmpty() throws IOException {
        final com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap mutable =
                new com.yandex.yoctodb.util.mutable.impl.BitSetIndexToIndexMultiMap(0);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        assertEquals(
                V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode(),
                buf.getInt());

        final IndexToIndexMultiMap result =
                BitSetIndexToIndexMultiMap.from(buf);

        assertEquals(0, result.getKeysCount());
    }

    @Test
    public void getFrom() throws IOException {
        final IndexToIndexMultiMap index = build();

        final BitSet dest = LongArrayBitSet.zero(DOCS);
        index.getFrom(dest, DOCS / 4);

        assertEquals(DOCS / 2, dest.cardinality());
        for (int i = DOCS / 2; i < DOCS; i++)
            assertTrue(dest.get(i));
    }

    @Test
    public void getTo() throws IOException {
        final IndexToIndexMultiMap index = build();

        final BitSet dest = LongArrayBitSet.zero(DOCS);
        index.getTo(dest, DOCS / 4);

        assertEquals(DOCS / 2, dest.cardinality());
        for (int i = 0; i < DOCS / 2; i++)
            assertTrue(dest.get(i));
    }

    @Test
    public void getBetween() throws IOException {
        final IndexToIndexMultiMap index = build();

        final BitSet dest = LongArrayBitSet.zero(DOCS);
        index.getBetween(dest, DOCS / 8, DOCS * 3 / 8);

        assertEquals(DOCS / 2, dest.cardinality());
        for (int i = DOCS / 4; i < DOCS * 3 / 4; i++)
            assertTrue(dest.get(i));
    }

    @Test
    public void string() throws IOException {
        final IndexToIndexMultiMap index = build();
        assertTrue(index.toString().contains(Integer.toString(DOCS / 2)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void ascendingUnsupported() throws IOException {
        build().ascending(LongArrayBitSet.one(DOCS));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void descendingUnsupported() throws IOException {
        build().descending(LongArrayBitSet.one(DOCS));
    }
}
