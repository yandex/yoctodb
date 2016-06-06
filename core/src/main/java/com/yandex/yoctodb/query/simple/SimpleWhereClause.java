/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import com.yandex.yoctodb.immutable.IndexedDatabase;
import com.yandex.yoctodb.query.*;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

/**
 * Where clause
 *
 * @author incubos
 */
@NotThreadSafe
public final class SimpleWhereClause implements Where {
    @NotNull
    private final SimpleSelect select;

    public SimpleWhereClause(
            @NotNull
            final SimpleSelect delegate) {
        this.select = delegate;
    }

    @NotNull
    @Override
    public Where and(
            @NotNull
            final Condition condition) {
        return select.where(condition);
    }

    // Delegated

    @Override
    public int getSkip() {
        return select.getSkip();
    }

    @Override
    public int getLimit() {
        return select.getLimit();
    }

    @Override
    public boolean hasSorting() {
        return select.hasSorting();
    }

    @NotNull
    @Override
    public Where where(
            @NotNull
            final Condition condition) {
        return select.where(condition);
    }

    @NotNull
    @Override
    public OrderBy orderBy(
            @NotNull
            final Order order) {
        return select.orderBy(order);
    }

    @NotNull
    @Override
    public Select skip(final int skip) {
        return select.skip(skip);
    }

    @NotNull
    @Override
    public Select limit(final int limit) {
        return select.limit(limit);
    }

    @Nullable
    @Override
    public BitSet filteredUnlimited(
            @NotNull
            final IndexedDatabase database,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        return select.filteredUnlimited(database, bitSetPool);
    }

    @NotNull
    @Override
    public Iterator<? extends ScoredDocument<?>> sortedUnlimited(
            @NotNull
            final BitSet docs,
            @NotNull
            final IndexedDatabase database,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        return select.sortedUnlimited(docs, database, bitSetPool);
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @NotNull
    @Override
    public Where clone() {
        return new SimpleWhereClause(select.clone());
    }

    @Override
    public String toString() {
        return select.toString();
    }
}
