/*
 * (C) YANDEX LLC, 2014-2016
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
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.yandex.yoctodb.v1.V1DatabaseFormat.MultiMapType.ROARING_BIT_SET_BASED;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link RoaringBitSetIndexToIndexMultiMap}
 *
 * @author incubos
 */
public class RoaringBitSetIndexToIndexMultiMapTest {
    private final int DOCS = 128;

    private IndexToIndexMultiMap build() throws IOException {
        final TreeMultimap<Integer, Integer> elements = TreeMultimap.create();
        for (int i = 0; i < DOCS; i++) {
            elements.put(i / 2, i);
        }
        final com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap mutable =
                new com.yandex.yoctodb.util.mutable.impl.RoaringBitSetIndexToIndexMultiMap(
                        elements.asMap().values());

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        assertEquals(ROARING_BIT_SET_BASED.getCode(), buf.getInt());

        final IndexToIndexMultiMap result =
                RoaringBitSetIndexToIndexMultiMap.from(buf);

        assertEquals(DOCS / 2, result.getKeysCount());

        return result;
    }

    @Test
    public void buildEmpty() throws IOException {
        final com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap mutable =
                new com.yandex.yoctodb.util.mutable.impl.RoaringBitSetIndexToIndexMultiMap(
                        Collections.<Collection<Integer>>emptyList());

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        assertEquals(ROARING_BIT_SET_BASED.getCode(), buf.getInt());

        final IndexToIndexMultiMap result =
                RoaringBitSetIndexToIndexMultiMap.from(buf);

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

    @Test
    public void ascendingIterator() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().ascending(LongArrayBitSet.one(DOCS));
        for (int i = 0; i < DOCS / 2; i++) {
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
        final BitSet filter = LongArrayBitSet.zero(DOCS);
        final int step = 4;
        for (int i = 0; i < DOCS; i += step)
            filter.set(i);

        final Iterator<IntToIntArray> iter = build().ascending(filter);

        for (int i = 0; i < DOCS / 2; i += 2) {
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
        final BitSet valueFilter = LongArrayBitSet.zero(DOCS + 1);
        valueFilter.set(DOCS);
        final Iterator<IntToIntArray> iter = build().ascending(valueFilter);
        assertFalse(iter.hasNext());
        iter.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void ascendingRemoveUnsupported() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().ascending(LongArrayBitSet.one(DOCS));
        assertTrue(iter.hasNext());
        iter.remove();
    }

    @Test
    public void descendingIterator() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().descending(LongArrayBitSet.one(DOCS));
        for (int i = DOCS / 2 - 1; i >= 0; i--) {
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
        final BitSet filter = LongArrayBitSet.zero(DOCS);
        final int step = 4;
        for (int i = 0; i < DOCS; i += step)
            filter.set(i);

        final Iterator<IntToIntArray> iter = build().descending(filter);

        for (int i = DOCS / 2 - 2; i >= 0; i -= 2) {
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
        final BitSet valueFilter = LongArrayBitSet.zero(DOCS + 1);
        valueFilter.set(DOCS);
        final Iterator<IntToIntArray> iter = build().descending(valueFilter);
        assertFalse(iter.hasNext());
        iter.next();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void descendingRemoveUnsupported() throws IOException {
        final Iterator<IntToIntArray> iter =
                build().descending(LongArrayBitSet.one(DOCS));
        assertTrue(iter.hasNext());
        iter.remove();
    }
}
