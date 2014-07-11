/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.immutable.util;

import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.immutable.IndexToIndexMap;
import com.yandex.yoctodb.util.immutable.impl.IntIndexToIndexMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author svyatoslav
 *         Date: 21.11.13
 */
public class IntIndexToIndexMapTest {
    @Test
    public void simpleTest() throws IOException {
        for (int counter = 1; counter < 10000; counter++) {
            final int size = counter;
            final ByteBuffer buf = prepareData(size);
            final IndexToIndexMap map = IntIndexToIndexMap.from(buf);
            Assert.assertEquals(size, map.size());
            for (int i = 0; i < size; i++) {
                Assert.assertEquals(size - i, map.get(i));
            }
        }
    }

    private ByteBuffer prepareData(final int size) throws IOException {
        final com.yandex.yoctodb.util.mutable.IndexToIndexMap indexToIndexMap =
                new com.yandex.yoctodb.util.mutable.impl.IntIndexToIndexMap();
        for (int i = 0; i < size; i++) {
            indexToIndexMap.put(i, size - i);
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        indexToIndexMap.writeTo(os);
        Assert.assertEquals(os.size(), indexToIndexMap.getSizeInBytes());
        return ByteBuffer.wrap(os.toByteArray());
    }
}
