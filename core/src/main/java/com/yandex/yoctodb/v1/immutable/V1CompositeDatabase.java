/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable;

import com.google.common.collect.Iterators;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.ScoredDocument;
import com.yandex.yoctodb.util.buf.Buffer;
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
    private final List<V1Database> databases;
    private final int[] documentOffsets;
    private final int documentCount;

    public V1CompositeDatabase(
            @NotNull
            final Collection<V1Database> databases) {
        this.databases = new ArrayList<V1Database>(databases);

        this.documentOffsets = new int[databases.size()];
        int documentCount = 0;
        for (int i = 0; i < documentOffsets.length; i++) {
            documentOffsets[i] = documentCount;
            documentCount += this.databases.get(i).getDocumentCount();
        }
        this.documentCount = documentCount;
    }

    @Override
    public int getDocumentCount() {
        return this.documentCount;
    }

    @NotNull
    @Override
    public Buffer getDocument(final int i) {
        assert 0 <= i && i < this.documentCount;

        final int offsetIndex = Arrays.binarySearch(documentOffsets, i);
        final int dbIndex;
        if (offsetIndex >= 0)
            dbIndex = offsetIndex;
        else
            dbIndex = -offsetIndex - 2;

        return databases.get(dbIndex).getDocument(i - documentOffsets[dbIndex]);
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
                    new ArrayList<Iterator<? extends ScoredDocument<?>>>(
                            databases.size());
            for (V1Database database : databases) {
                final BitSet docs = query.filteredUnlimited(database);

                if (docs == null) {
                    continue;
                }

                assert !docs.isEmpty();

                results.add(query.sortedUnlimited(docs, database));
            }

            if (results.isEmpty()) {
                return;
            }

            iterator =
                    Iterators.mergeSorted(
                            results,
                            SCORED_DOCUMENT_COMPARATOR
                    );
        } else {
            iterator =
                    Iterators.concat(
                            new FilterResultIterator(
                                    query,
                                    databases.iterator()));
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
                    new ArrayList<Iterator<? extends ScoredDocument<?>>>(
                            databases.size());
            for (V1Database database : databases) {
                final BitSet docs = query.filteredUnlimited(database);
                if (docs != null) {
                    assert !docs.isEmpty();

                    final int count = docs.cardinality();
                    if (count == database.getDocumentCount()) {
                        results.add(
                                query.sortedUnlimited(
                                        new ReadOnlyOneBitSet(
                                                database.getDocumentCount()),
                                        database
                                )
                        );
                    } else {
                        results.add(query.sortedUnlimited(docs, database));
                    }
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
            final List<DatabaseDocs> results =
                    new ArrayList<DatabaseDocs>(databases.size());
            for (V1Database database : databases) {
                final BitSet docs = query.filteredUnlimited(database);
                if (docs != null) {
                    assert !docs.isEmpty();

                    final int count = docs.cardinality();
                    if (count == database.getDocumentCount()) {
                        results.add(
                                new DatabaseDocs(
                                        database,
                                        new ReadOnlyOneBitSet(
                                                database.getDocumentCount())
                                )
                        );
                    } else {
                        results.add(new DatabaseDocs(database, docs));
                    }
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
        for (V1Database database : databases) {
            final BitSet docs = query.filteredUnlimited(database);
            if (docs != null) {
                count += docs.cardinality();
            }
        }
        return Math.min(
                Math.max(count - query.getSkip(), 0),
                query.getLimit());
    }
}
