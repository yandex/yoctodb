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

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import org.junit.Test;

import java.util.SortedSet;
import java.util.TreeSet;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link VariableLengthByteArraySortedSet}
 *
 * @author incubos
 */
public class VariableLengthByteArraySortedSetTest {
    @Test
    public void indexingFixed() {
        final SortedSet<UnsignedByteArray> elements = new TreeSet<>();
        final int size = 3;
        for (int i = 0; i < size; i++)
            elements.add(from(i));
        final ByteArraySortedSet set =
                new VariableLengthByteArraySortedSet(
                        elements);
        for (int i = 0; i < size; i++)
            assertEquals(i, set.indexOf(from(i)));
    }

    @Test
    public void indexingVariable() {
        final SortedSet<UnsignedByteArray> elements = new TreeSet<>();
        final byte[] e1 = new byte[]{1, 1, 0};
        final byte[] e2 = new byte[]{1, 0};
        final byte[] e3 = new byte[]{0};
        final byte[] e4 = new byte[]{};
        elements.add(from(e1));
        elements.add(from(e2));
        elements.add(from(e3));
        elements.add(from(e4));
        final ByteArraySortedSet set =
                new VariableLengthByteArraySortedSet(
                        elements);
        assertEquals(0, set.indexOf(from(e4)));
        assertEquals(1, set.indexOf(from(e3)));
        assertEquals(2, set.indexOf(from(e2)));
        assertEquals(3, set.indexOf(from(e1)));
    }

    @Test(expected = AssertionError.class)
    public void notFound() {
        final SortedSet<UnsignedByteArray> elements = new TreeSet<>();
        final int size = 3;
        for (int i = 0; i < size; i++)
            elements.add(from(i));
        final ByteArraySortedSet set =
                new VariableLengthByteArraySortedSet(
                        elements);
        set.indexOf(from(size + 1));
    }

    @Test
    public void string() {
        final SortedSet<UnsignedByteArray> elements = new TreeSet<>();
        final int size = 10;
        for (long i = 0; i < size; i++)
            elements.add(from(i));
        final ByteArraySortedSet set =
                new VariableLengthByteArraySortedSet(
                        elements);
        final String s = set.toString();
        assertTrue(s.contains(Integer.toString(size)));
    }
}
