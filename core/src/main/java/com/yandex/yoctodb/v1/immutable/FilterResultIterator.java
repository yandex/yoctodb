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

import com.yandex.yoctodb.immutable.IndexedDatabase;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.ScoredDocument;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Iterator;

/**
 * Applies query filtering to each of the databases
 *
 * @author incubos
 */
public class FilterResultIterator
        implements Iterator<Iterator<? extends ScoredDocument<?>>> {
    @NotNull
    private final Query query;
    @NotNull
    private final Iterator<? extends IndexedDatabase> databases;
    @NotNull
    private final ArrayBitSetPool bitSetPool;

    public FilterResultIterator(
            @NotNull
            final Query query,
            @NotNull
            final Iterator<? extends IndexedDatabase> databases,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        this.query = query;
        this.databases = databases;
        this.bitSetPool = bitSetPool;
    }

    @Override
    public boolean hasNext() {
        return databases.hasNext();
    }

    @NotNull
    @Override
    public Iterator<? extends ScoredDocument<?>> next() {
        final IndexedDatabase db = databases.next();
        final BitSet docs = query.filteredUnlimited(db, bitSetPool);
        if (docs == null) {
            return Collections.emptyIterator();
        } else {
            return query.sortedUnlimited(docs, db, bitSetPool);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(
                "Removal is not supported");
    }
}
