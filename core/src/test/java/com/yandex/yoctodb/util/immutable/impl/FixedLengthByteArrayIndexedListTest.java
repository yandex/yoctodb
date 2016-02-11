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

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link FixedLengthByteArrayIndexedList}
 *
 * @author incubos
 */
public class FixedLengthByteArrayIndexedListTest {
    private final int VALUES = 128;

    private ByteArrayIndexedList build() throws IOException {
        final Collection<UnsignedByteArray> elements =
                new LinkedList<UnsignedByteArray>();
        for (long i = 0L; i < VALUES; i++) {
            elements.add(from(i));
        }

        final com.yandex.yoctodb.util.mutable.ByteArrayIndexedList mutable =
                new com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArrayIndexedList(elements);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        final ByteArrayIndexedList result =
                FixedLengthByteArrayIndexedList.from(buf);

        assertEquals(VALUES, result.size());

        return result;
    }

    @Test
    public void string() throws IOException {
        assertTrue(build().toString().contains(Integer.toString(VALUES)));
    }
}
