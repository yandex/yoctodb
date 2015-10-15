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
import com.yandex.yoctodb.immutable.FilterableIndex;
import com.yandex.yoctodb.immutable.Payload;
import com.yandex.yoctodb.immutable.SortableIndex;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.QueryContext;
import com.yandex.yoctodb.query.ScoredDocument;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.LongArrayBitSet;
import com.yandex.yoctodb.util.mutable.impl.ReadOnlyOneBitSet;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Immutable {@link Database} implementation in V1 format
 *
 * @author incubos
 */
@Immutable
public final class V1Database implements QueryContext {
    @NotNull
    private final Payload payload;
    @NotNull
    private final Map<String, FilterableIndex> filters;
    @NotNull
    private final Map<String, SortableIndex> sorters;

    // BitSet cache
    private final ThreadLocal<BitSet> ONES =
            new ThreadLocal<BitSet>() {
                @Override
                protected BitSet initialValue() {
                    return LongArrayBitSet.zero(payload.getSize());
                }
            };
    private final ThreadLocal<BitSet> ZEROS =
            new ThreadLocal<BitSet>() {
                @Override
                protected BitSet initialValue() {
                    return LongArrayBitSet.zero(payload.getSize());
                }
            };

    public V1Database(
            @NotNull
            final Payload payload,
            @NotNull
            final Map<String, FilterableIndex> filters,
            @NotNull
            final Map<String, SortableIndex> sorters) {
        this.payload = payload;
        this.filters =
                Collections.unmodifiableMap(
                        new HashMap<String, FilterableIndex>(filters));
        this.sorters =
                Collections.unmodifiableMap(
                        new HashMap<String, SortableIndex>(sorters));
    }

    @NotNull
    @Override
    public Buffer getDocument(final int i) {
        return payload.getPayload(i);
    }

    @Override
    public int getDocumentCount() {
        return payload.getSize();
    }

    @Nullable
    @Override
    public FilterableIndex getFilter(
            @NotNull
            final String fieldName) {
        return filters.get(fieldName);
    }

    @NotNull
    @Override
    public SortableIndex getSorter(
            @NotNull
            final String fieldName) {
        assert sorters.containsKey(fieldName);

        return sorters.get(fieldName);
    }

    @Override
    public void execute(
            @NotNull
            final Query query,
            @NotNull
            final DocumentProcessor processor) {
        final BitSet docs = query.filteredUnlimited(this);
        if (docs == null) {
            return;
        }

        final Iterator<? extends ScoredDocument<?>> unlimited =
                query.sortedUnlimited(docs, this);

        if (query.getSkip() != 0) {
            Iterators.advance(unlimited, query.getSkip());
        }

        final Iterator<? extends ScoredDocument<?>> limited;
        if (query.getLimit() == Integer.MAX_VALUE) {
            limited = unlimited;
        } else {
            limited = Iterators.limit(unlimited, query.getLimit());
        }

        while (limited.hasNext()) {
            if (!processor.process(limited.next().getDocument(), this)) {
                return;
            }
        }
    }

    /**
     * The same as {@link #execute(com.yandex.yoctodb.query.Query,
     * com.yandex.yoctodb.query.DocumentProcessor)}, but returns also unlimited
     * document count
     */
    @Override
    public int executeAndUnlimitedCount(
            @NotNull
            final Query query,
            @NotNull
            final DocumentProcessor processor) {
        final BitSet docs = query.filteredUnlimited(this);
        if (docs == null) {
            return 0;
        }

        assert !docs.isEmpty();

        final int result = docs.cardinality();
        final Iterator<? extends ScoredDocument<?>> unlimited;
        if (result == getDocumentCount()) {
            unlimited =
                    query.sortedUnlimited(
                            new ReadOnlyOneBitSet(getDocumentCount()),
                            this);
        } else {
            unlimited = query.sortedUnlimited(docs, this);
        }

        if (query.getSkip() != 0) {
            Iterators.advance(unlimited, query.getSkip());
        }

        final Iterator<? extends ScoredDocument<?>> limited;
        if (query.getLimit() == Integer.MAX_VALUE) {
            limited = unlimited;
        } else {
            limited = Iterators.limit(unlimited, query.getLimit());
        }

        while (limited.hasNext()) {
            if (!processor.process(limited.next().getDocument(), this)) {
                return result;
            }
        }

        return result;
    }

    @Override
    public int count(
            @NotNull
            final Query query) {
        final BitSet docs = query.filteredUnlimited(this);
        if (docs == null) {
            return 0;
        } else {
            return Math.min(
                    Math.max(
                            docs.cardinality() - query.getSkip(),
                            0),
                    query.getLimit()
            );
        }
    }

    @NotNull
    @Override
    public BitSet getZeroBitSet() {
        final BitSet result = ZEROS.get();
        result.clear();
        return result;
    }

    @NotNull
    @Override
    public BitSet getOneBitSet() {
        final BitSet result = ONES.get();
        result.set();
        return result;
    }

    @Override
    public String toString() {
        return "V1Database{" +
               "payload=" + payload +
               ", filters=" + filters +
               ", sorters=" + sorters +
               '}';
    }
}
