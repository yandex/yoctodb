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
import java.util.Collections;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link BitSetIndexToIndexMultiMap}
 *
 * @author incubos
 */
public class BitSetIndexToIndexMultiMapTest {
    @Test(expected = IllegalArgumentException.class)
    public void negativeDocuments() {
        new BitSetIndexToIndexMultiMap(
                Collections.<Collection<Integer>>emptyList(),
                -1);
    }

    @Test(expected = AssertionError.class)
    public void wrongDocument() throws IOException {
        new BitSetIndexToIndexMultiMap(
                singletonList(singletonList(1)),
                1)
                .writeTo(new ByteArrayOutputStream());
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeValue() throws IOException {
        new BitSetIndexToIndexMultiMap(
                singletonList(singletonList(1)),
                -1)
                .writeTo(new ByteArrayOutputStream());
    }

    @Test
    public void string() {
        final TreeMultimap<Integer, Integer> elements = TreeMultimap.create();
        final int documents = 10;
        for (int i = 0; i < documents; i++)
            elements.put(i / 2, i);
        final IndexToIndexMultiMap set =
                new BitSetIndexToIndexMultiMap(
                        elements.asMap().values(),
                        documents);
        final String text = set.toString();
        assertTrue(text.contains(Integer.toString(documents / 2)));
        assertTrue(text.contains(Integer.toString(documents)));
        set.getSizeInBytes();
        assertTrue(text.contains(Integer.toString(documents / 2)));
        assertTrue(text.contains(Integer.toString(documents)));
    }
}
