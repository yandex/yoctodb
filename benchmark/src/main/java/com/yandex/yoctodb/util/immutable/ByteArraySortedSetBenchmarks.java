package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collection;

import static com.yandex.yoctodb.util.immutable.ByteArraySortedSetBenchmarkData.*;

/**
 * Benchmarks for {@link ByteArraySortedSet} implementations
 *
 * @author incubos
 */
public class ByteArraySortedSetBenchmarks {
    private long measure(@NotNull final ByteArraySortedSet set, @NotNull final Collection<Buffer> queries) {
        long res = 0;
        for (Buffer b : queries)
            res += set.indexOf(b);
        return res;
    }

    @Benchmark
    public void random_8bytes_64K_fixed(final Blackhole bh) {
        bh.consume(measure(RANDOM_8B_64K.fixedIndex(), RANDOM_8B_64K.queries()));
    }

    @Benchmark
    public void random_8bytes_64K_variable(final Blackhole bh) {
        bh.consume(measure(RANDOM_8B_64K.variableIndex(), RANDOM_8B_64K.queries()));
    }

    @Benchmark
    public void random_8bytes_64K_trie(final Blackhole bh) {
        bh.consume(measure(RANDOM_8B_64K.trieIndex(), RANDOM_8B_64K.queries()));
    }

    @Benchmark
    public void random_prefixed_string_100K_fixed(final Blackhole bh) {
        bh.consume(measure(RANDOM_STRING_WITH_PREFIX_25B_100K.fixedIndex(), RANDOM_STRING_WITH_PREFIX_25B_100K.queries()));
    }

    @Benchmark
    public void random_prefixed_string_100K_variable(final Blackhole bh) {
        bh.consume(measure(RANDOM_STRING_WITH_PREFIX_25B_100K.variableIndex(), RANDOM_STRING_WITH_PREFIX_25B_100K.queries()));
    }

    @Benchmark
    public void random_prefixed_string_100K_trie(final Blackhole bh) {
        bh.consume(measure(RANDOM_STRING_WITH_PREFIX_25B_100K.trieIndex(), RANDOM_STRING_WITH_PREFIX_25B_100K.queries()));
    }

    @Benchmark
    public void price_fixed(final Blackhole bh) {
        bh.consume(measure(PRICES_4B.fixedIndex(), PRICES_4B.queries()));
    }

    @Benchmark
    public void price_variable(final Blackhole bh) {
        bh.consume(measure(PRICES_4B.variableIndex(), PRICES_4B.queries()));
    }

    @Benchmark
    public void price_trie(final Blackhole bh) {
        bh.consume(measure(PRICES_4B.trieIndex(), PRICES_4B.queries()));
    }
}
