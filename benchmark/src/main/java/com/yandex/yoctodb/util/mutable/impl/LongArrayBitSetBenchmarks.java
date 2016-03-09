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

import com.yandex.yoctodb.util.mutable.BitSet;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Some benchmarks for {@link LongArrayBitSet}
 *
 * @author incubos
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class LongArrayBitSetBenchmarks {
    private static final int SIZE = 64 * 1024;

    private static void nextSetInt(
            final BitSet set,
            final Blackhole bh) {
        int i = 0;
        while (i >= 0) {
            final int j = i;
            i = set.nextSetBit(i + 1);
            bh.consume(j);
        }
    }

    @State(Scope.Benchmark)
    public static class OneState {
        public final BitSet set = LongArrayBitSet.one(SIZE);
    }

    @Benchmark
    public void onesIterator(
            final OneState state,
            final Blackhole bh) {
        nextSetInt(state.set, bh);
    }

    @State(Scope.Benchmark)
    public static class InterleavedState {
        public final BitSet set = LongArrayBitSet.zero(SIZE);

        @Setup
        public void prepare() {
            for (int i = 0; i < SIZE; i += 2)
                set.set(i);
        }
    }

    @Benchmark
    public void interleavedIterator(
            final InterleavedState state,
            final Blackhole bh) {
        nextSetInt(state.set, bh);
    }

    @State(Scope.Benchmark)
    public static class RandomState {
        public final BitSet set = LongArrayBitSet.zero(SIZE);

        @Setup
        public void prepare() {
            final Random r = new Random();
            for (int i = 0; i < SIZE; i += 2)
                if (r.nextBoolean())
                    set.set(i);
        }
    }

    @Benchmark
    public void randomIterator(
            final RandomState state,
            final Blackhole bh) {
        nextSetInt(state.set, bh);
    }

    @State(Scope.Benchmark)
    public static class ZeroState {
        public final BitSet set = LongArrayBitSet.zero(SIZE);
    }

    @Benchmark
    public void zerosIterator(
            final ZeroState state,
            final Blackhole bh) {
        nextSetInt(state.set, bh);
    }
}
