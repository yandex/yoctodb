package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

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