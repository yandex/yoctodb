/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link IntIndexToIndexMultiMap}
 *
 * @author incubos
 */
public class IntIndexToIndexMultiMapTest {
    private final int VALUES = 128;

    private IndexToIndexMultiMap build() throws IOException {
        final com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap mutable =
                new com.yandex.yoctodb.util.mutable.impl.IntIndexToIndexMultiMap();
        for (int i = 0; i < VALUES; i++) {
            mutable.put(i / 2, i);
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        assertEquals(
                V1DatabaseFormat.MultiMapType.LIST_BASED.getCode(),
                buf.getInt());

        final IndexToIndexMultiMap result =
                IntIndexToIndexMultiMap.from(buf);

        assertEquals(VALUES / 2, result.getKeysCount());

        return result;
    }

    @Test
    public void string() throws IOException {
        assertTrue(build().toString().contains(Integer.toString(VALUES / 2)));
    }

    @Test
    public void ascendingIterator() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().ascending(LongArrayBitSet.one(VALUES));
        for (int i = 0; i < VALUES / 2; i++) {
            assertTrue(iter.hasNext());
            final IntToIntArray e = iter.next();
            assertEquals(i, e.getKey());
            assertEquals(2, e.getCount());
            assertArrayEquals(new int[]{i * 2, i * 2 + 1}, e.getValues());
        }
        assertFalse(iter.hasNext());
    }

    @Test
    public void ascendingSparseIterator() throws IOException {
        final BitSet filter = LongArrayBitSet.zero(VALUES);
        final int step = 4;
        for (int i = 0; i < VALUES; i += step)
            filter.set(i);

        final Iterator<IntToIntArray> iter = build().ascending(filter);

        for (int i = 0; i < VALUES / 2; i += 2) {
            assertTrue(iter.hasNext());
            final IntToIntArray e = iter.next();
            assertEquals(i, e.getKey());
            assertEquals(1, e.getCount());
            assertEquals(i * 2, e.getValues()[0]);
        }
        assertFalse(iter.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void ascendingEmptyIterator() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().ascending(LongArrayBitSet.zero(VALUES));
        assertFalse(iter.hasNext());
        iter.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void ascendingRemoveUnsupported() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().ascending(LongArrayBitSet.one(VALUES));
        assertTrue(iter.hasNext());
        iter.remove();
    }

    @Test
    public void descendingIterator() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().descending(LongArrayBitSet.one(VALUES));
        for (int i = VALUES / 2 - 1; i >= 0; i--) {
            assertTrue(iter.hasNext());
            final IntToIntArray e = iter.next();
            assertEquals(i, e.getKey());
            assertEquals(2, e.getCount());
            assertArrayEquals(new int[]{i * 2, i * 2 + 1}, e.getValues());
        }
        assertFalse(iter.hasNext());
    }

    @Test
    public void descendingSparseIterator() throws IOException {
        final BitSet filter = LongArrayBitSet.zero(VALUES);
        final int step = 4;
        for (int i = 0; i < VALUES; i += step)
            filter.set(i);

        final Iterator<IntToIntArray> iter = build().descending(filter);

        for (int i = VALUES / 2 - 2; i >= 0; i -= 2) {
            assertTrue(iter.hasNext());
            final IntToIntArray e = iter.next();
            assertEquals(i, e.getKey());
            assertEquals(1, e.getCount());
            assertEquals(i * 2, e.getValues()[0]);
        }
        assertFalse(iter.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void descendingEmptyIterator() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().descending(LongArrayBitSet.zero(VALUES));
        assertFalse(iter.hasNext());
        iter.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void descendingRemoveUnsupported() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().descending(LongArrayBitSet.one(VALUES));
        assertTrue(iter.hasNext());
        iter.remove();
    }
}
