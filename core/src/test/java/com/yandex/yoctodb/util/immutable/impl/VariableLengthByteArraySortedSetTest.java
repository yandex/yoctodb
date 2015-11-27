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
import com.yandex.yoctodb.util.immutable.ByteArraySortedSet;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link VariableLengthByteArraySortedSet}
 *
 * @author incubos
 */
public class VariableLengthByteArraySortedSetTest {
    private final int VALUES = 128;

    private ByteArraySortedSet build() throws IOException {
        final com.yandex.yoctodb.util.mutable.ByteArraySortedSet mutable =
                new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet();
        for (int i = 0; i < VALUES; i++) {
            if (i % 2 == 0)
                mutable.add(from(i / 2));
            else
                mutable.add(from(i / 2L));
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        final ByteArraySortedSet result =
                VariableLengthByteArraySortedSet.from(buf);

        assertEquals(VALUES, result.size());

        return result;
    }

    @Test
    public void string() throws IOException {
        final String text = build().toString();
        assertTrue(text.contains(Integer.toString(VALUES)));
    }
}
