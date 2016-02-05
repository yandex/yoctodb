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

import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link IntIndexToIndexMultiMap}
 *
 * @author incubos
 */
public class IntIndexToIndexMultiMapTest {
    @Test(expected = IllegalArgumentException.class)
    public void negativeKey() {
        final IndexToIndexMultiMap set = new IntIndexToIndexMultiMap();
        set.put(-1, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeValue() {
        final IndexToIndexMultiMap set = new IntIndexToIndexMultiMap();
        set.put(0, -1);
    }

    @Test(expected = IllegalStateException.class)
    public void noncontinuous() throws IOException {
        final IndexToIndexMultiMap set = new IntIndexToIndexMultiMap();
        set.put(0, 0);
        set.put(2, 1);
        set.writeTo(new ByteArrayOutputStream());
    }

    @Test
    public void string() {
        final int documents = 10;
        final IndexToIndexMultiMap set = new IntIndexToIndexMultiMap();
        for (int i = 0; i < documents; i++)
            set.put(i / 2, i);
        final String text = set.toString();
        assertTrue(text.contains(Integer.toString(documents / 2)));
        assertTrue(text.contains(Integer.toString(documents)));
        set.getSizeInBytes();
        assertTrue(text.contains(Integer.toString(documents / 2)));
        assertTrue(text.contains(Integer.toString(documents)));
    }
}
