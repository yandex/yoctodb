/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable;

import com.google.common.collect.Iterators;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.immutable.DocumentProvider;
import com.yandex.yoctodb.immutable.IndexedDatabase;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.ScoredDocument;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.ReadOnlyOneBitSet;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Composite database for a set of {@link V1Database}s
 *
 * @author incubos
 */
@Immutable
public final class V1CompositeDatabase implements Database {
    private static final Comparator<ScoredDocument> SCORED_DOCUMENT_COMPARATOR = new Comparator<ScoredDocument>() {
        @SuppressWarnings("unchecked")
        @Override
        public int compare(
                final ScoredDocument o1,
                final ScoredDocument o2) {
            return o1.compareTo(o2);
        }
    };
    @NotNull
    private final List<IndexedDatabase> databases;
    @NotNull
    private final ArrayBitSetPool bitSetPool;
    @NotNull
    private final int[] documentOffsets;
    private final int documentCount;

    public V1CompositeDatabase(
            @NotNull
            final Collection<? extends IndexedDatabase> databases,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        this.databases = new ArrayList<>(databases);
        this.documentOffsets = new int[databases.size()];
        int documentCount = 0;
        int i = 0;
        for (DocumentProvider db : this.databases) {
            documentOffsets[i] = documentCount;
            documentCount += db.getDocumentCount();
            i++;
        }
        this.documentCount = documentCount;
        this.bitSetPool = bitSetPool;
    }

    @Override
    public int getDocumentCount() {
        return this.documentCount;
    }

    private int databaseByDocIndex(final int i) {
        assert 0 <= i && i < this.documentCount;

        final int offsetIndex = Arrays.binarySearch(documentOffsets, i);

        return offsetIndex >= 0 ? offsetIndex : -offsetIndex - 2;
    }

    @NotNull
    @Override
    public Buffer getDocument(final int i) {
        final int dbIndex = databaseByDocIndex(i);
        return databases.get(dbIndex).getDocument(i - documentOffsets[dbIndex]);
    }

    @NotNull
    @Override
    public Buffer getFieldValue(
            final int document,
            @NotNull
            final String fieldName) {
        final int dbIndex = databaseByDocIndex(document);
        return databases.get(dbIndex)
                .getFieldValue(
                        document - documentOffsets[dbIndex],
                        fieldName);
    }

    @Override
    public long getLongValue(
            final int document,
            @NotNull
            final String fieldName) {
        final int dbIndex = databaseByDocIndex(document);
        return databases.get(dbIndex)
                .getLongValue(
                        document - documentOffsets[dbIndex],
                        fieldName);
    }

    @Override
    public int getIntValue(
            final int document,
            @NotNull
            final String fieldName) {
        final int dbIndex = databaseByDocIndex(document);
        return databases.get(dbIndex)
                .getIntValue(
                        document - documentOffsets[dbIndex],
                        fieldName
                );
    }

    @Override
    public short getShortValue(
            final int document,
            @NotNull
            final String fieldName) {
        final int dbIndex = databaseByDocIndex(document);
        return databases.get(dbIndex)
                .getShortValue(
                        document - documentOffsets[dbIndex],
                        fieldName
                );
    }

    @Override
    public char getCharValue(
            final int document,
            @NotNull
            final String fieldName) {
        final int dbIndex = databaseByDocIndex(document);
        return databases.get(dbIndex)
                .getCharValue(
                        document - documentOffsets[dbIndex],
                        fieldName
                );
    }

    @Override
    public byte getByteValue(
            final int document,
            @NotNull
            final String fieldName) {
        final int dbIndex = databaseByDocIndex(document);
        return databases.get(dbIndex)
                .getByteValue(
                        document - documentOffsets[dbIndex],
                        fieldName
                );
    }

    @Override
    public void execute(
            @NotNull
            final Query query,
            @NotNull
            final DocumentProcessor processor) {
        final Iterator<ScoredDocument<?>> iterator;

        // Doing merging iff there is sorting
        if (query.hasSorting()) {
            final List<Iterator<? extends ScoredDocument<?>>> results =
                    new ArrayList<>(
                            databases.size());
            for (IndexedDatabase db : databases) {
                final BitSet docs = query.filteredUnlimited(db, bitSetPool);

                if (docs == null) {
                    continue;
                }

                assert !docs.isEmpty();

                results.add(query.sortedUnlimited(docs, db, bitSetPool));
            }

            if (results.isEmpty()) {
                return;
            }

            iterator =
                    Iterators.mergeSorted(
                            results,
                            SCORED_DOCUMENT_COMPARATOR);
        } else {
            iterator =
                    Iterators.concat(
                            new FilterResultIterator(
                                    query,
                                    databases.iterator(),
                                    bitSetPool));
        }

        // Skipping values
        if (query.getSkip() != 0) {
            Iterators.advance(iterator, query.getSkip());
        }

        // Limited
        final Iterator<ScoredDocument<?>> limitedIterator;
        if (query.getLimit() == Integer.MAX_VALUE) {
            limitedIterator = iterator;
        } else {
            limitedIterator = Iterators.limit(iterator, query.getLimit());
        }

        while (limitedIterator.hasNext()) {
            final ScoredDocument<?> document = limitedIterator.next();
            if (!processor.process(
                    document.getDocument(),
                    document.getDatabase())) {
                return;
            }
        }
    }

    @Override
    public int executeAndUnlimitedCount(
            @NotNull
            final Query query,
            @NotNull
            final DocumentProcessor processor) {
        int result = 0;
        final Iterator<ScoredDocument<?>> iterator;

        // Doing merging iff there is sorting
        if (query.hasSorting()) {
            final List<Iterator<? extends ScoredDocument<?>>> results =
                    new ArrayList<>(
                            databases.size());
            for (IndexedDatabase db : databases) {
                final BitSet docs = query.filteredUnlimited(db, bitSetPool);
                if (docs != null) {
                    assert !docs.isEmpty();

                    final int dbSize = db.getDocumentCount();
                    final int count = docs.cardinality();
                    final BitSet filter;
                    if (count == dbSize) {
                        filter = new ReadOnlyOneBitSet(dbSize);
                    } else {
                        filter = docs;
                    }
                    results.add(
                            query.sortedUnlimited(
                                    filter,
                                    db,
                                    bitSetPool));
                    result += count;
                }
            }

            if (results.isEmpty()) {
                return 0;
            }

            iterator =
                    Iterators.mergeSorted(
                            results,
                            SCORED_DOCUMENT_COMPARATOR
                    );
        } else {
            final List<QueryContext> results =
                    new ArrayList<>(databases.size());
            for (IndexedDatabase db : databases) {
                final BitSet docs = query.filteredUnlimited(db, bitSetPool);
                if (docs != null) {
                    assert !docs.isEmpty();

                    final int dbSize = db.getDocumentCount();
                    final int count = docs.cardinality();
                    final BitSet filter;
                    if (count == dbSize) {
                        filter = new ReadOnlyOneBitSet(dbSize);
                    } else {
                        filter = docs;
                    }
                    results.add(new QueryContext(filter, db, bitSetPool));
                    result += count;
                }
            }

            if (results.isEmpty()) {
                return 0;
            }

            iterator =
                    Iterators.concat(
                            new SortResultIterator(
                                    query,
                                    results.iterator()));
        }

        // Skipping values
        if (query.getSkip() != 0) {
            Iterators.advance(iterator, query.getSkip());
        }

        // Limited
        final Iterator<ScoredDocument<?>> limitedIterator;
        if (query.getLimit() == Integer.MAX_VALUE) {
            limitedIterator = iterator;
        } else {
            limitedIterator = Iterators.limit(iterator, query.getLimit());
        }

        while (limitedIterator.hasNext()) {
            final ScoredDocument<?> document = limitedIterator.next();
            if (!processor.process(
                    document.getDocument(),
                    document.getDatabase())) {
                return result;
            }
        }

        return result;
    }

    @Override
    public int count(
            @NotNull
            final Query query) {
        int count = 0;
        for (IndexedDatabase db : databases) {
            final BitSet docs = query.filteredUnlimited(db, bitSetPool);
            if (docs != null) {
                count += docs.cardinality();
            }
        }

        return Math.min(
                Math.max(count - query.getSkip(), 0),
                query.getLimit());
    }
}
