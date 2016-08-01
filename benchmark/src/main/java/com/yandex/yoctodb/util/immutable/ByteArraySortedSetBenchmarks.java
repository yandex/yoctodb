package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.impl.FixedLengthByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArraySortedSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    private static final List<Buffer> elements;

    static {
        // Random
        final Random rnd = new Random();

        // Building elements
        final SortedSet<UnsignedByteArray> set = new TreeSet<>();
        for (int i = 0; i < ELEMENTS; i++) {
            final byte[] element = new byte[SIZE];
            rnd.nextBytes(element);
            set.add(UnsignedByteArrays.from(element));
        }

        // Building fixed

        {
            final com.yandex.yoctodb.util.mutable.ByteArraySortedSet mutable =
                    new com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet(
                            set);

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                mutable.writeTo(baos);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            final Buffer buf = Buffer.from(baos.toByteArray());

            fixed = FixedLengthByteArraySortedSet.from(buf);
        }

        // Building variable

        {
            final com.yandex.yoctodb.util.mutable.ByteArraySortedSet mutable =
                    new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet(
                            set);

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                mutable.writeTo(baos);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            final Buffer buf = Buffer.from(baos.toByteArray());

            variable = VariableLengthByteArraySortedSet.from(buf);
        }

        // Preparing queries

        elements = new ArrayList<>(set.size());
        for (UnsignedByteArray e: set) {
            elements.add(e.toByteBuffer());
        }

        Collections.shuffle(new ArrayList<>(elements), rnd);
    }

    private long measure(final ByteArraySortedSet set) {
        long res = 0;
        for (Buffer b: elements)
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
}
