/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.immutable.impl.IntIndexToIndexMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author svyatoslav
 */
public class IntIndexToIndexMapTest {
    @Test
    public void simpleTest() throws IOException {
        for (int counter = 1; counter < 10000; counter++) {
            final Buffer buf = prepareData(counter);
            final IndexToIndexMap map = IntIndexToIndexMap.from(buf);
            Assert.assertEquals(counter, map.size());
            for (int i = 0; i < counter; i++) {
                Assert.assertEquals(counter - i, map.get(i));
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
        return Buffer.from(os.toByteArray());
    }
}
