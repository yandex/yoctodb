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

import static org.junit.Assert.assertEquals;

public class TrieByteArraySortedSetTest {
    private static final List<String> keys = Arrays.asList(
            "a",
            "as",
            "ascending",
            "ask",
            "ass",
            "assign",
            "assignment",
            "be",
            "been",
            "car",
            "card",
            "care",
            "careful",
            "careless",
            "case",
            "cat",
            "door",
            "doorbelkhng",
            "doorbelking",
            "doorbell",
            "doorbelling",
            "xanthan",
            "xanthate",
            "xanthene",
            "xanthic",
            "xanthin",
            "xanthine",
            "xanthoma",
            "xebec",
            "xeme",
            "xenia",
            "xenial",
            "xenogamy",
            "xenolith",
            "xenology",
            "xenon",
            "xenotime",
            "yb",
            "ybeeex"
    );
    private static final com.yandex.yoctodb.util.mutable.impl.TrieByteArraySortedSet tbs;
    private static final com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet keysSet;
    private static final com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet emptySet;

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
        keysSet = com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet.from(Buffer.from(buffer));
        emptySet = com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet.from(Buffer.from(ByteBuffer.allocate(4)));
    }


    @Test
    public void size() {
        assertEquals(keys.size(), keysSet.size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void get() {
        keysSet.get(0);
    }

    @Test
    public void indexOf() {
        for (int i = 0; i < keys.size(); i++) {
            assertEquals(i, keysSet.indexOf(UnsignedByteArrays.from(keys.get(i)).toByteBuffer()));
        }

        assertEquals(-1, keysSet.indexOf(UnsignedByteArrays.from("").toByteBuffer()));
        assertEquals(-1, keysSet.indexOf(UnsignedByteArrays.from("carder").toByteBuffer()));
        assertEquals(-1, keysSet.indexOf(UnsignedByteArrays.from("asa").toByteBuffer()));
        assertEquals(-1, keysSet.indexOf(UnsignedByteArrays.from("asf").toByteBuffer()));
        assertEquals(-1, keysSet.indexOf(UnsignedByteArrays.from("assignm").toByteBuffer()));
        assertEquals(-1, keysSet.indexOf(UnsignedByteArrays.from("asz").toByteBuffer()));
        assertEquals(-1, keysSet.indexOf(UnsignedByteArrays.from("beg").toByteBuffer()));
        assertEquals(-1, keysSet.indexOf(UnsignedByteArrays.from("cal").toByteBuffer()));
        assertEquals(-1, keysSet.indexOf(UnsignedByteArrays.from("z").toByteBuffer()));

        assertEquals(-1, emptySet.indexOf(UnsignedByteArrays.from("z").toByteBuffer()));
    }

    @Test
    public void indexOfGreaterThan() {
        assertEquals(-1, emptySet.indexOfGreaterThan(UnsignedByteArrays.from("").toByteBuffer(), true, -1));

        assertEquals(0, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("").toByteBuffer(), true, -1));
        assertEquals(-1, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("z").toByteBuffer(), false, -1));
        assertEquals(0, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("a").toByteBuffer(), true, -1));
        assertEquals(1, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("a").toByteBuffer(), false, -1));
        assertEquals(2, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("asb").toByteBuffer(), true, -1));
        assertEquals(2, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("ascending").toByteBuffer(), true, -1));
        assertEquals(2, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("ascendin").toByteBuffer(), true, -1));
        assertEquals(2, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("ascendiz").toByteBuffer(), true, -1));
        assertEquals(2, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("ascendia").toByteBuffer(), true, -1));
        assertEquals(3, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("ascendingz").toByteBuffer(), true, -1));
        assertEquals(3, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("ascending").toByteBuffer(), false, -1));
        assertEquals(3, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("asg").toByteBuffer(), false, -1));
        assertEquals(7, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("asz").toByteBuffer(), false, -1));
        assertEquals(7, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("assz").toByteBuffer(), false, -1));
        assertEquals(7, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("b").toByteBuffer(), false, -1));
        assertEquals(7, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("be").toByteBuffer(), true, -1));
        assertEquals(8, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("be").toByteBuffer(), false, -1));
        assertEquals(8, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("bee").toByteBuffer(), false, -1));
        assertEquals(8, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("bec").toByteBuffer(), false, -1));
        assertEquals(9, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("cab").toByteBuffer(), false, -1));
        assertEquals(16, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("caz").toByteBuffer(), false, -1));
        assertEquals(16, keysSet.indexOfGreaterThan(UnsignedByteArrays.from("catz").toByteBuffer(), false, -1));
    }

    @Test
    public void indexOfLessThan() {
        assertEquals(-1, emptySet.indexOfLessThan(UnsignedByteArrays.from("").toByteBuffer(), true, -1));

        assertEquals(-1, keysSet.indexOfLessThan(UnsignedByteArrays.from("").toByteBuffer(), true, -1));
        assertEquals(-1, keysSet.indexOfLessThan(UnsignedByteArrays.from("").toByteBuffer(), false, -1));
        assertEquals(keys.size() - 1, keysSet.indexOfLessThan(UnsignedByteArrays.from("z").toByteBuffer(), false, -1));
        assertEquals(0, keysSet.indexOfLessThan(UnsignedByteArrays.from("a").toByteBuffer(), true, -1));
        assertEquals(-1, keysSet.indexOfLessThan(UnsignedByteArrays.from("a").toByteBuffer(), false, -1));
        assertEquals(1, keysSet.indexOfLessThan(UnsignedByteArrays.from("asb").toByteBuffer(), true, -1));
        assertEquals(2, keysSet.indexOfLessThan(UnsignedByteArrays.from("ascending").toByteBuffer(), true, -1));
        assertEquals(1, keysSet.indexOfLessThan(UnsignedByteArrays.from("ascendin").toByteBuffer(), true, -1));
        assertEquals(2, keysSet.indexOfLessThan(UnsignedByteArrays.from("ascendiz").toByteBuffer(), true, -1));
        assertEquals(1, keysSet.indexOfLessThan(UnsignedByteArrays.from("ascendia").toByteBuffer(), true, -1));
        assertEquals(2, keysSet.indexOfLessThan(UnsignedByteArrays.from("ascendingz").toByteBuffer(), true, -1));
        assertEquals(1, keysSet.indexOfLessThan(UnsignedByteArrays.from("ascending").toByteBuffer(), false, -1));
        assertEquals(2, keysSet.indexOfLessThan(UnsignedByteArrays.from("asg").toByteBuffer(), false, -1));
        assertEquals(6, keysSet.indexOfLessThan(UnsignedByteArrays.from("asz").toByteBuffer(), false, -1));
        assertEquals(6, keysSet.indexOfLessThan(UnsignedByteArrays.from("assz").toByteBuffer(), false, -1));
        assertEquals(6, keysSet.indexOfLessThan(UnsignedByteArrays.from("b").toByteBuffer(), false, -1));
        assertEquals(7, keysSet.indexOfLessThan(UnsignedByteArrays.from("be").toByteBuffer(), true, -1));
        assertEquals(6, keysSet.indexOfLessThan(UnsignedByteArrays.from("be").toByteBuffer(), false, -1));
        assertEquals(7, keysSet.indexOfLessThan(UnsignedByteArrays.from("bee").toByteBuffer(), false, -1));
        assertEquals(7, keysSet.indexOfLessThan(UnsignedByteArrays.from("bec").toByteBuffer(), false, -1));
        assertEquals(8, keysSet.indexOfLessThan(UnsignedByteArrays.from("cab").toByteBuffer(), false, -1));
        assertEquals(15, keysSet.indexOfLessThan(UnsignedByteArrays.from("caz").toByteBuffer(), false, -1));
        assertEquals(15, keysSet.indexOfLessThan(UnsignedByteArrays.from("catz").toByteBuffer(), false, -1));
        assertEquals(16, keysSet.indexOfLessThan(UnsignedByteArrays.from("doorbel").toByteBuffer(), false, -1));
        assertEquals(16, keysSet.indexOfLessThan(UnsignedByteArrays.from("doorbela").toByteBuffer(), false, -1));
        assertEquals(31, keysSet.indexOfLessThan(UnsignedByteArrays.from("xenm").toByteBuffer(), false, -1));
        assertEquals(37, keysSet.indexOfLessThan(UnsignedByteArrays.from("ybd").toByteBuffer(), false, -1));
    }
}