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

import com.google.common.primitives.Ints;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link FixedLengthByteArraySortedSet}
 *
 * @author incubos
 */
public class FixedLengthByteArraySortedSetTest {
    @Test(expected = IllegalArgumentException.class)
    public void empty() {
        new FixedLengthByteArraySortedSet(new TreeSet<UnsignedByteArray>());
    }

    @Test
    public void indexing() {
        final SortedSet<UnsignedByteArray> elements =
                new TreeSet<>();
        final int size = 3;
        for (int i = 0; i < size; i++)
            elements.add(from(i));
        final ByteArraySortedSet set =
                new FixedLengthByteArraySortedSet(
                        elements);
        for (int i = 0; i < size; i++)
            assertEquals(i, set.indexOf(from(i)));
    }

    @Test(expected = AssertionError.class)
    public void expectFixed() throws IOException {
        final SortedSet<UnsignedByteArray> elements =
                new TreeSet<>();
        elements.add(from(1));
        elements.add(from(1L));
        new FixedLengthByteArraySortedSet(elements)
                .writeTo(new ByteArrayOutputStream());
    }

    @Test(expected = AssertionError.class)
    public void notFound() {
        final SortedSet<UnsignedByteArray> elements =
                new TreeSet<>();
        final int size = 3;
        for (int i = 0; i < size; i++)
            elements.add(from(i));
        final ByteArraySortedSet set =
                new FixedLengthByteArraySortedSet(
                        elements);
        set.indexOf(from(size + 1));
    }

    @Test
    public void string() {
        final SortedSet<UnsignedByteArray> elements =
                new TreeSet<>();
        final int size = 10;
        for (int i = 0; i < size; i++)
            elements.add(from(i));
        final ByteArraySortedSet set =
                new FixedLengthByteArraySortedSet(
                        elements);
        final String unfrozen = set.toString();
        assertTrue(unfrozen.contains(Integer.toString(size)));
        assertTrue(unfrozen.contains(Integer.toString(Ints.BYTES)));
        set.getSizeInBytes();
        final String frozen = set.toString();
        assertTrue(frozen.contains(Integer.toString(size)));
        assertTrue(frozen.contains(Integer.toString(Ints.BYTES)));
    }
}
