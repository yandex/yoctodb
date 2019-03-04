package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.impl.FixedLengthByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.TrieByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArraySortedSet;

import java.io.*;
import java.util.*;

class ByteArraySortedSetBenchmarkData {

    static class BenchmarkSet {
        private final FixedLengthByteArraySortedSet fixed;
        private final VariableLengthByteArraySortedSet variable;
        private final TrieByteArraySortedSet trie;
        private final Collection<Buffer> queries;

        private BenchmarkSet(SortedSet<UnsignedByteArray> keys, Collection<Buffer> queries) {
            this.queries = queries;

            // Building fixed
            fixed =
                    FixedLengthByteArraySortedSet.from(
                            persist(
                                    new com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet(keys)
                            )
                    );

            // Building variable
            variable =
                    VariableLengthByteArraySortedSet.from(
                            persist(
                                    new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet(keys)
                            )
                    );

            // Building trie
            trie =
                    TrieByteArraySortedSet.from(
                            persist(
                                    new com.yandex.yoctodb.util.mutable.impl.TrieByteArraySortedSet(keys)
                            )
                    );
        }

        public FixedLengthByteArraySortedSet fixedIndex() {
            return fixed;
        }

        public VariableLengthByteArraySortedSet variableIndex() {
            return variable;
        }

        public TrieByteArraySortedSet trieIndex() {
            return trie;
        }

        public Collection<Buffer> queries() {
            return queries;
        }
    }

    private static final int RANDOM_SIZE = 8;
    private static final int RANDOM_ELEMENTS = 64 * 1024;
    private static final String PREFIX = "SOME_LONG_PREFIX_";
    private static final int MIN_PRICE = 20000;
    private static final int MAX_PRICE = 250000000;

    static final BenchmarkSet RANDOM_8B_64K;
    static final BenchmarkSet RANDOM_STRING_WITH_PREFIX_25B_100K;
    static final BenchmarkSet PRICES_4B;

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

        {
            // Building elements
            final SortedSet<UnsignedByteArray> keys = new TreeSet<>();
            for (int i = 0; i < RANDOM_ELEMENTS; i++) {
                final byte[] element = new byte[RANDOM_SIZE];
                rnd.nextBytes(element);
                keys.add(UnsignedByteArrays.from(element));
            }

            // Preparing queries

            final List<Buffer> elements = new ArrayList<>(keys.size() * 2);
            for (UnsignedByteArray e : keys) {
                elements.add(e.toByteBuffer());
            }
            // (maybe?) false queries
            for (int i = 0; i < keys.size(); i++) {
                final byte[] element = new byte[RANDOM_SIZE];
                rnd.nextBytes(element);
                elements.add(UnsignedByteArrays.from(element).toByteBuffer());
            }


            Collections.shuffle(elements, rnd);

            RANDOM_8B_64K = new BenchmarkSet(keys, elements);
        }

        {
            // Building elements
            final SortedSet<UnsignedByteArray> keys = new TreeSet<>();
            for (int i = 0; i < 100_000; i++) {
                keys.add(UnsignedByteArrays.from(PREFIX + UUID.randomUUID().toString().substring(0, RANDOM_SIZE)));
            }

            // Preparing queries

            final List<Buffer> elements = new ArrayList<>(keys.size() * 2);
            for (UnsignedByteArray e : keys) {
                elements.add(e.toByteBuffer());
            }
            // (maybe?) false queries
            for (int i = 0; i < keys.size(); i++) {
                elements.add(
                        UnsignedByteArrays.from(PREFIX +
                                UUID.randomUUID().toString().substring(0, RANDOM_SIZE))
                                .toByteBuffer()
                );
            }

            Collections.shuffle(elements, rnd);

            RANDOM_STRING_WITH_PREFIX_25B_100K = new BenchmarkSet(keys, elements);
        }

        {
            // Building elements
            final SortedSet<UnsignedByteArray> keys = new TreeSet<>();

            for (int i = MIN_PRICE; i < MAX_PRICE; i += i / 10000) {
                keys.add(UnsignedByteArrays.from(i));
            }

            // Preparing queries

            final List<Buffer> elements = new ArrayList<>(keys.size() * 2);
            for (UnsignedByteArray e : keys) {
                elements.add(e.toByteBuffer());
            }
            // (maybe?) false queries
            for (int i = 0; i < keys.size(); i++) {
                elements.add(
                        UnsignedByteArrays.from(rnd.nextInt(MAX_PRICE))
                                .toByteBuffer()
                );
            }

            Collections.shuffle(elements, rnd);

            PRICES_4B = new BenchmarkSet(keys, elements);
        }
    }

}
