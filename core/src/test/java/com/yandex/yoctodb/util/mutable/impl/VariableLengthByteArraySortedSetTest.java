/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import org.junit.Test;

import java.util.NoSuchElementException;

import static com.yandex.yoctodb.util.UnsignedByteArrays.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link VariableLengthByteArraySortedSet}
 *
 * @author incubos
 */
public class VariableLengthByteArraySortedSetTest {
    @Test
    public void freeze() {
        new VariableLengthByteArraySortedSet().build();
    }

    @Test(expected = IllegalStateException.class)
    public void notFreezeTwice() {
        final AbstractByteArraySortedSet set =
                new VariableLengthByteArraySortedSet();
        set.build();
        set.build();
    }

    @Test(expected = IllegalStateException.class)
    public void frozenIsImmutable() {
        final ByteArraySortedSet set = new VariableLengthByteArraySortedSet();
        set.add(from(1));
        set.getSizeInBytes();
        set.add(from(2));
    }

    @Test
    public void indexingFixed() {
        final ByteArraySortedSet set = new VariableLengthByteArraySortedSet();
        final int elements = 3;
        for (int i = 0; i < elements; i++)
            set.add(from(i));
        for (int i = 0; i < elements; i++)
            assertEquals(i, set.indexOf(from(i)));
    }

    @Test
    public void indexingVariable() {
        final ByteArraySortedSet set = new VariableLengthByteArraySortedSet();
        final byte[] e1 = new byte[]{1, 1, 0};
        final byte[] e2 = new byte[]{1, 0};
        final byte[] e3 = new byte[]{0};
        final byte[] e4 = new byte[]{};
        set.add(raw(e1));
        set.add(raw(e2));
        set.add(raw(e3));
        set.add(raw(e4));
        assertEquals(0, set.indexOf(raw(e4)));
        assertEquals(1, set.indexOf(raw(e3)));
        assertEquals(2, set.indexOf(raw(e2)));
        assertEquals(3, set.indexOf(raw(e1)));
    }

    @Test(expected = NoSuchElementException.class)
    public void notFound() {
        final ByteArraySortedSet set = new VariableLengthByteArraySortedSet();
        final int elements = 3;
        for (int i = 0; i < elements; i++)
            set.add(from(i));
        set.indexOf(from(elements + 1));
    }

    @Test
    public void string() {
        final ByteArraySortedSet set = new VariableLengthByteArraySortedSet();
        final int elements = 10;
        for (long i = 0; i < elements; i++)
            set.add(from(i));
        final String unfrozen = set.toString();
        assertTrue(unfrozen.contains(Integer.toString(elements)));
        set.getSizeInBytes();
        final String frozen = set.toString();
        assertTrue(frozen.contains(Integer.toString(elements)));
    }
}
