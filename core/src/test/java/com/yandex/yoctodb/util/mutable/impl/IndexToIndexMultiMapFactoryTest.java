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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link IndexToIndexMultiMapFactory}
 *
 * @author incubos
 */
public class IndexToIndexMultiMapFactoryTest {
    @Test(expected = IllegalArgumentException.class)
    public void zeroValues() {
        IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(
                Collections.<Collection<Integer>>emptyList(),
                1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void zeroDocuments() {
        IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(
                singletonList(singletonList(0)),
                0);
    }

    @Test
    public void selective() {
        final TreeMultimap<Integer, Integer> elements = TreeMultimap.create();
        for (int i = 0; i < 1024; i++) {
            elements.put(i, i);
        }
        final IndexToIndexMultiMap map =
                IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(
                        elements.asMap().values(),
                        1024);
        assertTrue(map instanceof IntIndexToIndexMultiMap);
    }

    @Test
    public void nonSelective() {
        @SuppressWarnings("unchecked")
        final IndexToIndexMultiMap map =
                IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(
                        Arrays.asList(singletonList(0), singletonList(1)),
                        128);
        assertTrue(map instanceof BitSetIndexToIndexMultiMap);
    }
}
