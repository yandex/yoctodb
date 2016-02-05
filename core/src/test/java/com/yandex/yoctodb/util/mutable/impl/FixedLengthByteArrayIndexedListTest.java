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

import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import org.junit.Test;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link FixedLengthByteArrayIndexedList}
 *
 * @author incubos
 */
public class FixedLengthByteArrayIndexedListTest {
    @Test(expected = IllegalArgumentException.class)
    public void expectFixed() {
        final ByteArrayIndexedList set = new FixedLengthByteArrayIndexedList();
        set.add(from(1));
        set.add(from(1L));
    }

    @Test
    public void string() {
        final ByteArrayIndexedList set = new FixedLengthByteArrayIndexedList();
        final int elements = 10;
        for (int i = 0; i < elements; i++)
            set.add(from(i));
        final String text = set.toString();
        assertTrue(text.contains(Integer.toString(elements)));
        assertTrue(text.contains(Integer.toString(Integer.BYTES)));
    }
}
