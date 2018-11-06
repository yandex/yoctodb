package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.impl.FixedLengthByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArraySortedSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.util.*;

/**
 * Benchmarks for {@link ByteArraySortedSet} implementations
 *
 * @author incubos
 */
public class ByteArraySortedSetBenchmarks {
    private static final int SIZE = 8;
    private static final int ELEMENTS = 64 * 1024;

    private static final ByteArraySortedSet fixed;
    private static final ByteArraySortedSet variable;
    private static final ByteArraySortedSet trie;
    private static final List<Buffer> elements;

    private static Buffer persist(
            final com.yandex.yoctodb.util.mutable.ByteArraySortedSet mutable) {
        final File file;
        try {
            file = File.createTempFile("fixed", ".yoctodb");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        file.deleteOnExit();

        System.out.println("Size " + mutable.getSizeInBytes() + " bytes");

        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
            mutable.writeTo(os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            return Buffer.mmap(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        // Random
        final Random rnd = new Random();

        // Building elements
        final SortedSet<UnsignedByteArray> set = new TreeSet<>();
        final byte[] element = new byte[SIZE];
        for (int i = 0; i < ELEMENTS; i++) {
            rnd.nextBytes(element);
            set.add(UnsignedByteArrays.from(element));
        }

        // Building fixed
        fixed =
                FixedLengthByteArraySortedSet.from(
                        persist(
                                new com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet(
                                        set)));

        // Building variable
        variable =
                VariableLengthByteArraySortedSet.from(
                        persist(
                                new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet(
                                        set)));

        // Building trie
        trie =
                TrieByteArraySortedSet.from(
                        persist(
                                new com.yandex.yoctodb.util.mutable.impl.TrieByteArraySortedSet(set)
                        )
                );

        // Preparing queries

        elements = new ArrayList<>(set.size() * 2);
        for (UnsignedByteArray e : set) {
            elements.add(e.toByteBuffer());
        }
        // (maybe?) false queries
        for (int i = 0; i < set.size(); i++) {
            rnd.nextBytes(element);
            elements.add(UnsignedByteArrays.from(element).toByteBuffer());
        }

        Collections.shuffle(elements, rnd);
    }

    private long measure(final ByteArraySortedSet set) {
        long res = 0;
        for (Buffer b : elements)
            res += set.indexOf(b);
        return res;
    }

    @Benchmark
    public void fixed(final Blackhole bh) {
        bh.consume(measure(fixed));
    }

    @Benchmark
    public void variable(final Blackhole bh) {
        bh.consume(measure(variable));
    }

    @Benchmark
    public void trie(final Blackhole bh) {
        bh.consume(measure(trie));
    }
}
