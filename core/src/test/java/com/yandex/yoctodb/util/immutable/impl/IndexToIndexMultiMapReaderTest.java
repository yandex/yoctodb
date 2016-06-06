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

import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.Ints;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link IndexToIndexMultiMapReader}
 *
 * @author incubos
 */
public class IndexToIndexMultiMapReaderTest {

    @Test
    public void build() throws IOException {
        final TreeMultimap<Integer, Integer> elements = TreeMultimap.create();
        final int DOCS = 128;
        for (int i = 0; i < DOCS; i++) {
            elements.put(i / 2, i);
        }
        final com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap mutable =
                new com.yandex.yoctodb.util.mutable.impl.RoaringBitSetIndexToIndexMultiMap(
                        elements.asMap().values());

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        final IndexToIndexMultiMap result =
                IndexToIndexMultiMapReader.from(buf);

        assertTrue(result instanceof RoaringBitSetIndexToIndexMultiMap);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unsupported() throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        baos.write(Ints.toByteArray(Integer.MAX_VALUE));

        final Buffer buf = Buffer.from(baos.toByteArray());

        IndexToIndexMultiMapReader.from(buf);
    }
}
