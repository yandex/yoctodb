/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import com.google.common.collect.Iterators;
import com.yandex.yoctodb.immutable.SortableIndex;
import com.yandex.yoctodb.query.Order;
import com.yandex.yoctodb.query.QueryContext;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.mutable.BitSet;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
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
        if (orders[0].isAscending())
            baseIterator = indexes[0].ascending(docs);
        else
            baseIterator = indexes[0].descending(docs);

        this.chunk = Collections.emptyIterator();
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
        if (!chunk.hasNext())
            fillChunk();

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
            final Buffer[] sortValues =
                    new Buffer[sortValueIndexes.length];
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
