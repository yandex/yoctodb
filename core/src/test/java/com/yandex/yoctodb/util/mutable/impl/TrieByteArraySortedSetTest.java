package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.*;

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
}