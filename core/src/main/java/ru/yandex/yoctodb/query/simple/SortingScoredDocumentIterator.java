/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query.simple;

import com.google.common.collect.Iterators;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.immutable.IntToIntArray;
import ru.yandex.yoctodb.immutable.SortableIndex;
import ru.yandex.yoctodb.query.Order;
import ru.yandex.yoctodb.query.QueryContext;
import ru.yandex.yoctodb.util.mutable.BitSet;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * {@link Iterator} implementation producing {@link SimpleScoredDocument}s
 *
 * @author incubos
 */
@NotThreadSafe
public final class SortingScoredDocumentIterator
        implements Iterator<SimpleScoredDocument> {
    @NotNull
    private final QueryContext ctx;
    @NotNull
    private final SortableIndex[] indexes;
    @NotNull
    private final Order.SortOrder[] orders;
    @NotNull
    private final Iterator<IntToIntArray> baseIterator;
    @NotNull
    private Iterator<LocalScoredDocument> chunk;

    public SortingScoredDocumentIterator(
            @NotNull
            final QueryContext ctx,
            @NotNull
            final BitSet docs,
            @NotNull
            final List<Order> sorts) {
        assert !docs.isEmpty();
        assert !sorts.isEmpty();

        this.ctx = ctx;

        // Preparing sorting structures
        indexes = new SortableIndex[sorts.size()];
        orders = new Order.SortOrder[sorts.size()];
        {
            int i = 0;
            for (Order sort : sorts) {
                indexes[i] = ctx.getSorter(sort.getFieldName());
                orders[i] = sort.getOrder();
                i++;
            }
        }

        // Building base iterator
        switch (orders[0]) {
            case ASC:
                baseIterator = indexes[0].ascending(docs);
                break;
            case DESC:
                baseIterator = indexes[0].descending(docs);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported sort order: " + orders[0]);
        }

        this.chunk = Iterators.emptyIterator();
    }

    @Override
    public boolean hasNext() {
        return chunk.hasNext() || baseIterator.hasNext();
    }

    private int withOrder(
            final Order.SortOrder order,
            final int value) {
        if (order == Order.SortOrder.DESC)
            return -value;
        else
            return value;
    }

    private void fillChunk() {
        assert baseIterator.hasNext();

        final IntToIntArray taggedDocuments = baseIterator.next();
        final int[] ids = taggedDocuments.getValues();
        final int count = taggedDocuments.getCount();

        assert count > 0;

        final LocalScoredDocument[] docs = new LocalScoredDocument[count];

        for (int i = 0; i < count; i++) {
            final int id = ids[i];
            final int[] sortValueIndexes = new int[indexes.length];
            sortValueIndexes[0] =
                    withOrder(
                            orders[0],
                            taggedDocuments.getKey());
            for (int s = 1; s < sortValueIndexes.length; s++)
                sortValueIndexes[s] =
                        withOrder(
                                orders[s],
                                indexes[s].getSortValueIndex(id));

            docs[i] =
                    new LocalScoredDocument(
                            id,
                            new LocalScore(sortValueIndexes));
        }

        // Special case for good index selectivity
        if (docs.length == 1) {
            chunk = Iterators.singletonIterator(docs[0]);
        } else {
            Arrays.sort(docs);
            chunk = Iterators.forArray(docs);
        }
    }

    @Override
    public SimpleScoredDocument next() {
        assert hasNext();

        if (chunk.hasNext())
            return chunk.next().toScoredDocument();

        fillChunk();

        assert chunk.hasNext();

        return chunk.next().toScoredDocument();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removal is not supported");
    }

    private final class LocalScore implements Comparable<LocalScore> {
        @NotNull
        private final int[] sortValueIndexes;

        private LocalScore(
                @NotNull
                final int[] sortValueIndexes) {
            assert sortValueIndexes.length > 0;

            this.sortValueIndexes = sortValueIndexes;
        }

        @Override
        public int compareTo(
                @NotNull
                final LocalScore o) {
            assert sortValueIndexes.length == o.sortValueIndexes.length;

            for (int i = 0; i < sortValueIndexes.length; i++) {
                final int cmp =
                        Integer.compare(
                                sortValueIndexes[i],
                                o.sortValueIndexes[i]);
                if (cmp != 0) {
                    return cmp;
                }
            }

            return 0;
        }

        @NotNull
        public SimpleDocumentMultiScore toScore() {
            final ByteBuffer[] sortValues =
                    new ByteBuffer[sortValueIndexes.length];
            for (int i = 0; i < sortValueIndexes.length; i++) {
                sortValues[i] =
                        indexes[i].getSortValue(
                                Math.abs(sortValueIndexes[i]));
            }

            return new SimpleDocumentMultiScore(orders, sortValues);
        }
    }

    private final class LocalScoredDocument
            implements Comparable<LocalScoredDocument> {
        @NotNull
        private final LocalScore score;
        private final int id;

        private LocalScoredDocument(
                final int id,
                @NotNull
                final LocalScore score) {
            assert id >= 0;

            this.id = id;
            this.score = score;
        }

        @Override
        public int compareTo(
                @NotNull
                final LocalScoredDocument o) {
            return score.compareTo(o.score);
        }

        @NotNull
        public SimpleScoredDocument toScoredDocument() {
            return new SimpleScoredDocument(ctx, score.toScore(), id);
        }
    }
}
