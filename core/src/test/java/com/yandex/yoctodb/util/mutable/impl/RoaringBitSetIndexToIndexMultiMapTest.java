/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.collect.TreeMultimap;
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RoaringBitSetIndexToIndexMultiMap}
 *
 * @author incubos
 */
public class RoaringBitSetIndexToIndexMultiMapTest {
    @Test(expected = AssertionError.class)
    public void negativeValue() throws IOException {
        new RoaringBitSetIndexToIndexMultiMap(
                singletonList(singletonList(-1)))
                .writeTo(new ByteArrayOutputStream());
    }

    @Test(expected = RuntimeException.class)
    public void ioException() throws IOException {
        final Collection<Collection<Integer>> broken = mock(Collection.class);
        when(broken.iterator()).thenThrow(new RuntimeException("Test"));

        new RoaringBitSetIndexToIndexMultiMap(broken);
    }

    @Test
    public void string() throws IOException {
        final TreeMultimap<Integer, Integer> elements = TreeMultimap.create();
        final int documents = 10;
        for (int i = 0; i < documents; i++)
            elements.put(i / 2, i);
        final IndexToIndexMultiMap set =
                new RoaringBitSetIndexToIndexMultiMap(
                        elements.asMap().values());
        final String text = set.toString();
        assertTrue(text.contains(Integer.toString(documents / 2)));
    }
}
