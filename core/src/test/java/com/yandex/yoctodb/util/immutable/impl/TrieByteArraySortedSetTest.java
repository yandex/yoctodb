/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@code TrieByteArraySortedSet}
 *
 * @author Andrey Korzinev (ya-goodfella@yandex.com)
 */
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
    private static final int lastIndex = keys.size() - 1;

    private static final Collection<String> falseQueries = Arrays.asList(
            "",
            "z",
            "a",
            "a",
            "asb",
            "ascending",
            "ascendin",
            "ascendiz",
            "ascendia",
            "ascendingz",
            "ascending",
            "asg",
            "asz",
            "assz",
            "b",
            "be",
            "be",
            "bee",
            "bec",
            "cab",
            "caz",
            "catz",
            "doorbel",
            "doorbela",
            "xenm",
            "ybd",
            "zzz"
    );

    private static com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet mutableVariable;
    private static com.yandex.yoctodb.util.mutable.impl.TrieByteArraySortedSet mutableTrie;

    private static final com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet emptyTrieSet;
    private static final com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet trieSet;
    private static final com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArraySortedSet variableSet;
    private static final Collection<String> queries = new ArrayList<>(keys);

    static {
        final SortedSet<UnsignedByteArray> set = new TreeSet<>();
        for (String str : keys) {
            set.add(from(str));
            queries.add(str);
        }

        mutableTrie = new com.yandex.yoctodb.util.mutable.impl.TrieByteArraySortedSet(set);
        final ByteArrayOutputStream trieOutput = new ByteArrayOutputStream();
        try {
            mutableTrie.writeTo(trieOutput);
        } catch (IOException e) {
            e.printStackTrace();
        }

        mutableVariable = new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet(set);
        final ByteArrayOutputStream variableOutput = new ByteArrayOutputStream();
        try {
            mutableVariable.writeTo(variableOutput);
        } catch (IOException e) {
            e.printStackTrace();
        }

        emptyTrieSet = com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet.from(
                Buffer.from(ByteBuffer.allocate(4))
        );
        trieSet = com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet.from(
                Buffer.from(trieOutput.toByteArray())
        );
        variableSet = com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArraySortedSet.from(
                Buffer.from(variableOutput.toByteArray())
        );

        queries.addAll(falseQueries);
    }

    @Test
    public void size() {
        assertEquals(keys.size(), trieSet.size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void get() {
        trieSet.get(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getLongUnsafe() {
        trieSet.getLongUnsafe(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getIntUnsafe() {
        trieSet.getIntUnsafe(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getShortUnsafe() {
        trieSet.getShortUnsafe(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getCharUnsafe() {
        trieSet.getCharUnsafe(0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getByteUnsafe() {
        trieSet.getByteUnsafe(0);
    }

    @Test
    public void indexOfMutable() {
        for (String q : keys) {
            assertEquals(
                    String.format("for query <%s>",  q),
                    mutableVariable.indexOf(from(q)),
                    mutableTrie.indexOf(from(q))
            );
        }
    }

    @Test
    public void indexOf() {
        for (String q : queries) {
            assertEquals(
                    String.format("for query <%s>", q),
                    variableSet.indexOf(query(q)),
                    trieSet.indexOf(query(q))
            );
        }
    }

    @Test
    public void indexOfEmpty() {
        for (String q : queries) {
            assertEquals(
                    String.format("for query <%s>", q),
                    -1,
                    emptyTrieSet.indexOf(query(q))
            );
        }
    }

    @Test
    public void indexOfGreaterThanEmpty() {
        for (String q : queries) {
            assertEquals(
                    String.format("for query <%s>", q),
                    -1,
                    emptyTrieSet.indexOfGreaterThan(query(q), true, lastIndex)
            );
        }
    }

    @Test
    public void indexOfLessThanEmpty() {
        for (String q : queries) {
            assertEquals(
                    String.format("for query <%s>", q),
                    -1,
                    emptyTrieSet.indexOfLessThan(query(q), true, 0)
            );
        }
    }

    @NotNull
    private static Buffer query(String z) {
        return from(z).toByteBuffer();
    }

    @Test
    public void indexOfGreaterThan() {
        for (String q : queries) {
            assertEquals(
                    String.format("for query <%s>", q),
                    variableSet.indexOfGreaterThan(query(q), false, lastIndex),
                    trieSet.indexOfGreaterThan(query(q), false, lastIndex)
            );
        }
    }

    @Test
    public void indexOfGreaterThanOrEquals() {
        for (String q : queries) {
            assertEquals(
                    String.format("for query <%s>", q),
                    variableSet.indexOfGreaterThan(query(q), true, lastIndex),
                    trieSet.indexOfGreaterThan(query(q), true, lastIndex)
            );
        }
    }

    @Test
    public void indexOfLessThan() {
        for (String q : queries) {
            assertEquals(
                    String.format("for query <%s>", q),
                    variableSet.indexOfLessThan(query(q), false, 0),
                    trieSet.indexOfLessThan(query(q), false, 0)
            );
        }
    }

    @Test
    public void indexOfLessThanOrEquals() {
        for (String q : queries) {
            assertEquals(
                    String.format("for query <%s>", q),
                    variableSet.indexOfLessThan(query(q), true, 0),
                    trieSet.indexOfLessThan(query(q), true, 0)
            );
        }
    }
}
