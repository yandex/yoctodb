/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable.util;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.util.immutable.impl.IntIndexToIndexMultiMap;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author svyatoslav
 */
public class IntIndexToIndexMultiMapTest {
    @Test
    public void simpleTest() throws IOException {
        final int keys = 128;
        final int values = 128;
        final Buffer buf = prepareData(keys, values);
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

    private Buffer prepareData(
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
        return Buffer.from(os.toByteArray());
    }
}
