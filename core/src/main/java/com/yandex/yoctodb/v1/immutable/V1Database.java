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
import com.yandex.yoctodb.immutable.*;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.ScoredDocument;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.ReadOnlyOneBitSet;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Immutable {@link Database} implementation in V1 format
 *
 * @author incubos
 */
@Immutable
public final class V1Database implements IndexedDatabase {
    @NotNull
    private final Payload payload;
    @NotNull
    private final Map<String, FilterableIndex> filters;
    @NotNull
    private final Map<String, SortableIndex> sorters;
    @NotNull
    private final ArrayBitSetPool bitSetPool;

    public V1Database(
            @NotNull
            final Payload payload,
            @NotNull
            final Map<String, FilterableIndex> filters,
            @NotNull
            final Map<String, SortableIndex> sorters,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        this.payload = payload;
        this.filters = unmodifiableMap(new HashMap<>(filters));
        this.sorters = unmodifiableMap(new HashMap<>(sorters));
        this.bitSetPool = bitSetPool;
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

    @NotNull
    @Override
    public Buffer getFieldValue(
            final int document,
            @NotNull
            final String fieldName) {
        final SortableIndex sorter = getSorter(fieldName);

        return sorter.getSortValue(sorter.getSortValueIndex(document));
    }

    @Override
    public void execute(
            @NotNull
            final Query query,
            @NotNull
            final DocumentProcessor processor) {
        final BitSet docs = query.filteredUnlimited(this, bitSetPool);
        if (docs == null) {
            return;
        }

        final Iterator<? extends ScoredDocument<?>> unlimited =
                query.sortedUnlimited(docs, this, bitSetPool);

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

    @Override
    public int executeAndUnlimitedCount(
            @NotNull
            final Query query,
            @NotNull
            final DocumentProcessor processor) {
        final BitSet docs = query.filteredUnlimited(this, bitSetPool);
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
                            this,
                            bitSetPool);
        } else {
            unlimited = query.sortedUnlimited(docs, this, bitSetPool);
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
        final BitSet docs = query.filteredUnlimited(this, bitSetPool);
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
}
