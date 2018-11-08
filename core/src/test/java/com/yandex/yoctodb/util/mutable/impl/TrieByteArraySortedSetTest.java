/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@code TrieByteArraySortedSet}
 *
 * @author Andrey Korzinev (ya-goodfella@yandex.com)
 */
public class TrieByteArraySortedSetTest {
    private static final TrieByteArraySortedSet tbs;

    static {
        List<String> lst = Arrays.asList("a", "as", "ass", "assignment", "car", "card", "care", "careful", "carl", "cars", "cat");

        SortedSet<UnsignedByteArray> set = new TreeSet<>();
        for (String str : lst) {
            set.add(UnsignedByteArrays.from(str));
        }

        tbs = new TrieByteArraySortedSet(set);
    }

    @Test
    public void indexOf() {
        assertEquals(3, tbs.indexOf(UnsignedByteArrays.from("assignment")));
    }

    @Test(expected = NoSuchElementException.class)
    public void invalidIndexOf() {
        tbs.indexOf(UnsignedByteArrays.from("z"));
    }

    @Test(expected = NoSuchElementException.class)
    public void invalidIndexOf2() {
        tbs.indexOf(UnsignedByteArrays.from("assignm"));
    }

    @Test(expected = NoSuchElementException.class)
    public void invalidIndexOf3() {
        tbs.indexOf(UnsignedByteArrays.from("ca"));
    }
}