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
import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.QueryContext;
import com.yandex.yoctodb.query.ScoredDocument;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.ReadOnlyOneBitSet;

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
    private final Collection<V1Database> databases;

    public V1CompositeDatabase(
            @NotNull
            final Collection<V1Database> databases) {
        this.databases = new ArrayList<V1Database>(databases);
    }

    @Override
    public int getDocumentCount() {
        int result = 0;
        for (V1Database database : databases) {
            result += database.getDocumentCount();
        }
        return result;
    }

    @NotNull
    @Override
    public Buffer getDocument(final int i) {
        assert 0 <= i && i < getDocumentCount();

        int id = i;
        for (V1Database database : databases) {
            final int count = database.getDocumentCount();
            if (id < count) {
                return database.getDocument(id);
            } else {
                id -= count;
            }
        }

        throw new IllegalStateException("Couldn't find document");
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
                            new Iterator<Iterator<? extends ScoredDocument<?>>>() {
                                private final Iterator<V1Database> dbs =
                                        databases.iterator();

                                @Override
                                public boolean hasNext() {
                                    return dbs.hasNext();
                                }

                                @Override
                                public Iterator<? extends ScoredDocument<?>> next() {
                                    final QueryContext ctx = dbs.next();
                                    final BitSet docs = query.filteredUnlimited(
                                            ctx);
                                    if (docs == null) {
                                        return Iterators.emptyIterator();
                                    } else {
                                        return query.sortedUnlimited(docs, ctx);
                                    }
                                }

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException(
                                            "Removal is not supported");
                                }
                            }
                    );
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
                            new Iterator<Iterator<? extends ScoredDocument<?>>>() {
                                private final Iterator<DatabaseDocs> dbs =
                                        results.iterator();

                                @Override
                                public boolean hasNext() {
                                    return dbs.hasNext();
                                }

                                @Override
                                public Iterator<? extends ScoredDocument<?>> next() {
                                    final DatabaseDocs db = dbs.next();
                                    return query.sortedUnlimited(
                                            db.docs,
                                            db.ctx);
                                }

                                @Override
                                public void remove() {
                                    throw new UnsupportedOperationException(
                                            "Removal is not supported");
                                }
                            }
                    );
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

    private static final class DatabaseDocs {
        @NotNull
        public final QueryContext ctx;
        @NotNull
        public final BitSet docs;

        private DatabaseDocs(
                @NotNull
                final QueryContext ctx,
                @NotNull
                final BitSet docs) {
            this.ctx = ctx;
            this.docs = docs;
        }
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
