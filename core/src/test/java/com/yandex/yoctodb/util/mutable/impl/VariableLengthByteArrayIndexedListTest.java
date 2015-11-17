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

import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import org.junit.Test;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link VariableLengthByteArrayIndexedList}
 *
 * @author incubos
 */
public class VariableLengthByteArrayIndexedListTest {
    @Test
    public void string() {
        final ByteArrayIndexedList set =
                new VariableLengthByteArrayIndexedList();
        final int elements = 10;
        for (int i = 0; i < elements; i++)
            set.add(from(i));
        final String text = set.toString();
        assertTrue(text.contains(Integer.toString(elements)));
    }
}
