/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import com.google.common.collect.TreeMultimap;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.impl.IntIndexToIndexMultiMap;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.junit.Assert;
import org.junit.Test;

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
            final Set<Integer> expected = new HashSet<>();
            expected.add((keys - i) % values);
            expected.add((2 * keys - i) % values);
            expected.add((3 * keys - i) % values);

            final BitSet result = LongArrayBitSet.zero(values);
            map.get(result, i);
            final Set<Integer> actual = new HashSet<>();
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
        final TreeMultimap<Integer, Integer> elements = TreeMultimap.create();
        for (int i = 0; i < keys; i++) {
            //same elements
            elements.put(i, (keys - i) % values);
            elements.put(i, (keys - i) % values);
            elements.put(i, (keys - i) % values);
            elements.put(i, (keys - i) % values);

            elements.put(i, (2 * keys - i) % values);
            elements.put(i, (3 * keys - i) % values);
        }
        final com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap indexToIndexMultiMap =
                new com.yandex.yoctodb.util.mutable.impl.IntIndexToIndexMultiMap(
                        elements.asMap().values());

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        indexToIndexMultiMap.writeTo(os);
        Assert.assertEquals(os.size(), indexToIndexMultiMap.getSizeInBytes());
        return Buffer.from(os.toByteArray());
    }
}
