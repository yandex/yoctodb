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
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.impl.IntIndexToIndexMultiMap;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author svyatoslav
 *         Date: 21.11.13
 */
public class IntIndexToIndexMultiMapTest {
    @Test
    public void simpleTest() throws IOException {
        final int keys = 128;
        final int values = 128;
        final ByteBuffer buf = prepareData(keys, values);
        final int code = buf.getInt();
        Assert.assertEquals(V1DatabaseFormat.MultiMapType.LIST_BASED.getCode(), code);
        final IndexToIndexMultiMap map = IntIndexToIndexMultiMap.from(buf);
        Assert.assertEquals(keys, map.getKeysCount());
        for (int i = 0; i < keys; i++) {
            final Set<Integer> expected = new HashSet<Integer>();
            expected.add((keys - i) % values);
            expected.add((2 * keys - i) % values);
            expected.add((3 * keys - i) % values);

            final BitSet result = LongArrayBitSet.zero(values);
            map.get(result, i);
            final Set<Integer> actual = new HashSet<Integer>();
            int id = 0;
            while (id < values && (id = result.nextSetBit(id)) >= 0) {
                actual.add(id++);
            }
            Assert.assertEquals(expected, actual);
        }
    }

    private ByteBuffer prepareData(
            final int keys,
            final int values) throws IOException {
        final com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap indexToIndexMultiMap =
                new com.yandex.yoctodb.util.mutable.impl.IntIndexToIndexMultiMap();
        for (int i = 0; i < keys; i++) {
            //same elements
            indexToIndexMultiMap.add(i, (keys - i) % values);
            indexToIndexMultiMap.add(i, (keys - i) % values);
            indexToIndexMultiMap.add(i, (keys - i) % values);
            indexToIndexMultiMap.add(i, (keys - i) % values);

            indexToIndexMultiMap.add(i, (2 * keys - i) % values);
            indexToIndexMultiMap.add(i, (3 * keys - i) % values);
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        indexToIndexMultiMap.writeTo(os);
        Assert.assertEquals(os.size(), indexToIndexMultiMap.getSizeInBytes());
        return ByteBuffer.wrap(os.toByteArray());
    }
}
