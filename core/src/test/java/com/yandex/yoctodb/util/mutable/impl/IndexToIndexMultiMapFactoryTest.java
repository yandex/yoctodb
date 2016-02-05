/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import static org.junit.Assert.*;

import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import org.junit.Test;

/**
 * Unit tests for {@link IndexToIndexMultiMapFactory}
 *
 * @author incubos
 */
public class IndexToIndexMultiMapFactoryTest {
    @Test(expected = IllegalArgumentException.class)
    public void zeroValues() {
        IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void zeroDocuments() {
        IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(1, 0);
    }

    @Test
    public void selective() {
        final IndexToIndexMultiMap map =
                IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(
                        1024,
                        1024);
        assertTrue(map instanceof IntIndexToIndexMultiMap);
    }

    @Test
    public void nonSelective() {
        final IndexToIndexMultiMap map =
                IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap(2, 128);
        assertTrue(map instanceof BitSetIndexToIndexMultiMap);
    }
}
