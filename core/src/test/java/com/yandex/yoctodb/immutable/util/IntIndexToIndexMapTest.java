/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable.util;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.immutable.IndexToIndexMap;
import com.yandex.yoctodb.util.immutable.impl.IntIndexToIndexMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author svyatoslav
 *         Date: 21.11.13
 */
public class IntIndexToIndexMapTest {
    @Test
    public void simpleTest() throws IOException {
        for (int counter = 1; counter < 10000; counter++) {
            final int size = counter;
            final Buffer buf = prepareData(size);
            final IndexToIndexMap map = IntIndexToIndexMap.from(buf);
            Assert.assertEquals(size, map.size());
            for (int i = 0; i < size; i++) {
                Assert.assertEquals(size - i, map.get(i));
            }
        }
    }

    private Buffer prepareData(final int size) throws IOException {
        final com.yandex.yoctodb.util.mutable.IndexToIndexMap indexToIndexMap =
                new com.yandex.yoctodb.util.mutable.impl.IntIndexToIndexMap();
        for (int i = 0; i < size; i++) {
            indexToIndexMap.put(i, size - i);
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        indexToIndexMap.writeTo(os);
        Assert.assertEquals(os.size(), indexToIndexMap.getSizeInBytes());
        return Buffer.wrap(os.toByteArray());
    }
}
