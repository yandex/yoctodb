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

import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.QueryContext;
import com.yandex.yoctodb.query.ScoredDocument;
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
    private final Iterator<? extends QueryContext> dbs;

    public FilterResultIterator(
            @NotNull
            final Query query,
            @NotNull
            final Iterator<? extends QueryContext> dbs) {
        this.query = query;
        this.dbs = dbs;
    }

    @Override
    public boolean hasNext() {
        return dbs.hasNext();
    }

    @NotNull
    @Override
    public Iterator<? extends ScoredDocument<?>> next() {
        final QueryContext ctx = dbs.next();
        final BitSet docs = query.filteredUnlimited(
                ctx);
        if (docs == null) {
            return Collections.emptyIterator();
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
