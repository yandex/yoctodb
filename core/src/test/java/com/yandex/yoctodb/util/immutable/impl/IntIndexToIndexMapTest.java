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

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.IndexToIndexMap;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link IntIndexToIndexMap}
 *
 * @author incubos
 */
public class IntIndexToIndexMapTest {
    private final int VALUES = 128;

    private IndexToIndexMap build() throws IOException {
        final com.yandex.yoctodb.util.mutable.IndexToIndexMap mutable =
                new com.yandex.yoctodb.util.mutable.impl.IntIndexToIndexMap();
        for (int i = 0; i < VALUES; i++) {
            mutable.put(i, i * 2);
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        mutable.writeTo(baos);

        final Buffer buf = Buffer.from(baos.toByteArray());

        final IndexToIndexMap result =
                IntIndexToIndexMap.from(buf);

        assertEquals(VALUES, result.size());

        return result;
    }

    @Test
    public void string() throws IOException {
        assertTrue(build().toString().contains(Integer.toString(VALUES)));
    }
}
