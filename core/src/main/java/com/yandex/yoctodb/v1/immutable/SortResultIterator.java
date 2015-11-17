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
import com.yandex.yoctodb.query.ScoredDocument;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
 * Applies query sorting to each of the databases
 *
 * @author incubos
 */
public class SortResultIterator
        implements Iterator<Iterator<? extends ScoredDocument<?>>> {
    @NotNull
    private final Query query;
    @NotNull
    private final Iterator<DatabaseDocs> dbs;

    public SortResultIterator(
            @NotNull
            final Query query,
            @NotNull
            final Iterator<DatabaseDocs> dbs) {
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
