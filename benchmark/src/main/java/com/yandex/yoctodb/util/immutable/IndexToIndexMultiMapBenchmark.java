package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.impl.*;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.util.mutable.impl.ThreadLocalCachedArrayBitSetPool;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.util.*;

import static com.yandex.yoctodb.v1.V1DatabaseFormat.MultiMapType.*;
import static com.yandex.yoctodb.util.mutable.impl.IndexToIndexMultiMapFactory.buildIndexToIndexMultiMap;

public class IndexToIndexMultiMapBenchmark {
    private static int KEYS_COUNT = 8192;
    private static int MIN_DOCS_COUNT = 1;
    private static int MAX_DOCS_COUNT = 10;

    private static final BitSetIndexToIndexMultiMap bitSetIndex;
    private static final IntIndexToIndexMultiMap listIndex;
    private static final AscendingBitSetIndexToIndexMultiMap ascendingIndex;
    private static final Collection<Collection<Integer>> valueToDocuments;
    private static final int documentsCount;
    private static final ArrayBitSetPool bitSetPool;

    private static Buffer persist(
            final com.yandex.yoctodb.util.OutputStreamWritable writable) {
        final File file;
        try {
            file = File.createTempFile(UUID.randomUUID().toString(), ".yoctodb");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        file.deleteOnExit();

        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
            writable.writeTo(os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            return Buffer.mmap(file).advance(Integer.BYTES).slice();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        Random rand = new Random();

        Iterator<Integer> documentsIterator = new Iterator<Integer>() {
            private int current = 0;

            @Override
            public boolean hasNext() {
                return current <= Integer.MAX_VALUE;
            }

            @Override
            public Integer next() {
                if (hasNext()) {
                    return current++;
                }

                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
            }
        };

        valueToDocuments = new ArrayList<>(KEYS_COUNT);

        for (int i = 0; i < KEYS_COUNT; i++) {
            final List<Integer> documents = new ArrayList<>();
            for (int j = rand.nextInt(MIN_DOCS_COUNT + MAX_DOCS_COUNT) - MIN_DOCS_COUNT; j >= 0; j--) {
                documents.add(documentsIterator.next());
            }

            valueToDocuments.add(documents);
        }

        documentsCount = documentsIterator.next();

        bitSetIndex = BitSetIndexToIndexMultiMap.from(
                persist(buildIndexToIndexMultiMap(LONG_ARRAY_BIT_SET_BASED, valueToDocuments, documentsCount))
        );

        listIndex = IntIndexToIndexMultiMap.from(
                persist(buildIndexToIndexMultiMap(LIST_BASED, valueToDocuments, documentsCount))
        );

        ascendingIndex = AscendingBitSetIndexToIndexMultiMap.from(
                persist(buildIndexToIndexMultiMap(ASCENDING_BIT_SET_BASED, valueToDocuments, documentsCount))
        );

        bitSetPool = new ThreadLocalCachedArrayBitSetPool(documentsCount, 1.0f);
    }

    private static long measureGet(final IndexToIndexMultiMap index) {
        final ArrayBitSet dest = bitSetPool.borrowSet(documentsCount);
        try {
            index.get(dest, 0);
            index.get(dest, KEYS_COUNT - 1);
            index.get(dest, KEYS_COUNT / 2);

            return dest.cardinality();
        } finally {
            bitSetPool.returnSet(dest);
        }
    }

    private static long measureFrom(final IndexToIndexMultiMap index) {
        final ArrayBitSet dest = bitSetPool.borrowSet(documentsCount);

        try {
            index.getFrom(dest, 0);
            long result = dest.cardinality();
            result = result << Integer.SIZE;

            dest.clear();

            index.getFrom(dest, KEYS_COUNT - 1);

            return result | dest.cardinality();
        } finally {
            bitSetPool.returnSet(dest);
        }
    }

    private static long measureUntil(final IndexToIndexMultiMap index) {
        final ArrayBitSet dest = bitSetPool.borrowSet(documentsCount);
        try {
            index.getTo(dest, 1);
            long result = dest.cardinality();
            result = result << Integer.SIZE;

            dest.clear();

            index.getTo(dest, KEYS_COUNT);

            return result | dest.cardinality();
        } finally {
            bitSetPool.returnSet(dest);
        }
    }

    @Benchmark
    public void bitSetGet(final Blackhole blackhole) {
        blackhole.consume(measureGet(bitSetIndex));
    }

    @Benchmark
    public void listSetGet(final Blackhole blackhole) {
        blackhole.consume(measureGet(listIndex));
    }

    @Benchmark
    public void ascendingSetGet(final Blackhole blackhole) {
        blackhole.consume(measureGet(ascendingIndex));
    }

    @Benchmark
    public void bitSetFrom(final Blackhole blackhole) {
        blackhole.consume(measureFrom(bitSetIndex));
    }

    @Benchmark
    public void listSetFrom(final Blackhole blackhole) {
        blackhole.consume(measureFrom(listIndex));
    }

    @Benchmark
    public void ascendingSetFrom(final Blackhole blackhole) {
        blackhole.consume(measureFrom(ascendingIndex));
    }

    @Benchmark
    public void bitSetUntil(final Blackhole blackhole) {
        blackhole.consume(measureUntil(bitSetIndex));
    }

    @Benchmark
    public void listSetUntil(final Blackhole blackhole) {
        blackhole.consume(measureUntil(listIndex));
    }

    @Benchmark
    public void ascendingSetUntil(final Blackhole blackhole) {
        blackhole.consume(measureUntil(ascendingIndex));
    }
}
