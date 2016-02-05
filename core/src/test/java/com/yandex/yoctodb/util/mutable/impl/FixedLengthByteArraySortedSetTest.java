/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import org.junit.Test;

import java.util.NoSuchElementException;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link FixedLengthByteArraySortedSet}
 *
 * @author incubos
 */
public class FixedLengthByteArraySortedSetTest {
    @Test
    public void freeze() {
        new FixedLengthByteArraySortedSet().build();
    }

    @Test(expected = IllegalStateException.class)
    public void notFreezeTwice() {
        final AbstractByteArraySortedSet set =
                new FixedLengthByteArraySortedSet();
        set.build();
        set.build();
    }

    @Test(expected = IllegalStateException.class)
    public void frozenIsImmutable() {
        final ByteArraySortedSet set = new FixedLengthByteArraySortedSet();
        set.add(from(1));
        set.getSizeInBytes();
        set.add(from(2));
    }

    @Test
    public void indexing() {
        final ByteArraySortedSet set = new FixedLengthByteArraySortedSet();
        final int elements = 3;
        for (int i = 0; i < elements; i++)
            set.add(from(i));
        for (int i = 0; i < elements; i++)
            assertEquals(i, set.indexOf(from(i)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void expectFixed() {
        final ByteArraySortedSet set = new FixedLengthByteArraySortedSet();
        set.add(from(1));
        set.add(from(1L));
    }

    @Test(expected = NoSuchElementException.class)
    public void notFound() {
        final ByteArraySortedSet set = new FixedLengthByteArraySortedSet();
        final int elements = 3;
        for (int i = 0; i < elements; i++)
            set.add(from(i));
        set.indexOf(from(elements + 1));
    }

    @Test
    public void string() {
        final ByteArraySortedSet set = new FixedLengthByteArraySortedSet();
        final int elements = 10;
        for (int i = 0; i < elements; i++)
            set.add(from(i));
        final String unfrozen = set.toString();
        assertTrue(unfrozen.contains(Integer.toString(elements)));
        assertTrue(unfrozen.contains(Integer.toString(Integer.BYTES)));
        set.getSizeInBytes();
        final String frozen = set.toString();
        assertTrue(frozen.contains(Integer.toString(elements)));
        assertTrue(frozen.contains(Integer.toString(Integer.BYTES)));
    }
}
