package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.*;

public class TrieByteArraySortedSetTest {
    private static final List<String> keys = Arrays.asList("a", "as", "ass", "assignment", "car", "card", "care", "careful", "carl", "cars", "cat");
    private static final com.yandex.yoctodb.util.mutable.impl.TrieByteArraySortedSet tbs;
    private static final com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet immutableSet;

    static {
        SortedSet<UnsignedByteArray> set = new TreeSet<>();
        for (String str : keys) {
            set.add(UnsignedByteArrays.from(str));
        }

        tbs = new com.yandex.yoctodb.util.mutable.impl.TrieByteArraySortedSet(set);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            tbs.writeTo(os);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteBuffer buffer = ByteBuffer.allocateDirect((int) tbs.getSizeInBytes());
        buffer.put(os.toByteArray());
        buffer.rewind();
        immutableSet = com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet.from(Buffer.from(buffer));
    }


    @Test
    public void size() {
        assertEquals(keys.size(), immutableSet.size());
    }

    @Test
    public void get() {
    }

    @Test
    public void indexOf() {
        assertEquals(5, immutableSet.indexOf(UnsignedByteArrays.from("card").toByteBuffer()));
    }

    @Test
    public void indexOfGreaterThan() {
        assertEquals(4, immutableSet.indexOfGreaterThan(UnsignedByteArrays.from("cas").toByteBuffer(), true, -1));
    }

    @Test
    public void indexOfLessThan() {
    }
}