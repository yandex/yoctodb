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
 * {@link com.yandex.yoctodb.util.immutable.ByteArrayIndexedList} with fixed size
 * elements
 *
 * @author irenkamalova
 */
public class FoldedByteArrayIndexedListTest {

    private ByteArrayIndexedList build(int size) throws IOException {
        final Collection<UnsignedByteArray> elements = new LinkedList<>();
        for (int i = 0; i < 50; i++) {
            if (i % 2 == 0)
                elements.add(from(i));
            else
                elements.add(from(i % 3 == 0 ? 1L : (long) i));
        }
        for (int i = 50; i < size; i++) {
            elements.add(from(i));
        }

        final com.yandex.yoctodb.util.mutable.ByteArrayIndexedList mutable =
                new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList(elements);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        final ByteArrayIndexedList result =
                FoldedByteArrayIndexedList.from(buf);

        assertEquals(size, result.size());

        return result;
    }

    @Test
    public void testBytesFolding() throws IOException {
        assertTrue(build(100).toString().contains(Integer.toString(100)));
    }

    @Test
    public void testShortFolding() throws IOException {
        assertTrue(build(1000).toString().contains(Integer.toString(1000)));
    }

    @Test
    public void testIntegerFolding() throws IOException {
        assertTrue(build(70000).toString().contains(Integer.toString(70000)));
    }
}
