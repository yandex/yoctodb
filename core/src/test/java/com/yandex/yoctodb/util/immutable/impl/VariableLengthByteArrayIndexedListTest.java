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
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link VariableLengthByteArrayIndexedList}
 *
 * @author incubos
 */
public class VariableLengthByteArrayIndexedListTest {
    private final int VALUES = 128;

    private ByteArrayIndexedList build() throws IOException {
        final com.yandex.yoctodb.util.mutable.ByteArrayIndexedList mutable =
                new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList();
        for (int i = 0; i < VALUES; i++) {
            if (i % 2 == 0)
                mutable.add(from(i));
            else
                mutable.add(from((long) i));
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        final ByteArrayIndexedList result =
                VariableLengthByteArrayIndexedList.from(buf);

        assertEquals(VALUES, result.size());

        return result;
    }

    @Test
    public void string() throws IOException {
        assertTrue(build().toString().contains(Integer.toString(VALUES)));
    }
}
