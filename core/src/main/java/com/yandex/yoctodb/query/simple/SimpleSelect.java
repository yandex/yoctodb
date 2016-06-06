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

import com.google.common.base.Joiner;
import com.yandex.yoctodb.immutable.IndexedDatabase;
import com.yandex.yoctodb.query.*;
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.ReadOnlyOneBitSet;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Simple {@link Select} implementation
 *
 * @author incubos
 */
@NotThreadSafe
public final class SimpleSelect implements Select {
    @NotNull
    private final List<Condition> conditions;
    @NotNull
    private final List<Order> sorts;
    private int skip;
    private int limit;

    private SimpleSelect(
            @NotNull
            final List<Condition> conditions,
            @NotNull
            final List<Order> sorts,
            final int skip,
            final int limit) {
        this.conditions = conditions;
        this.sorts = sorts;
        this.skip = skip;
        this.limit = limit;
    }

    public SimpleSelect() {
        this(
                new LinkedList<Condition>(),
                new LinkedList<Order>(),
                0,
                Integer.MAX_VALUE);
    }

    @NotNull
    @Override
    public Where where(
            @NotNull
            final Condition condition) {
        conditions.add(condition);
        return new SimpleWhereClause(this);
    }

    @NotNull
    @Override
    public OrderBy orderBy(
            @NotNull
            final Order order) {
        sorts.add(order);
        return new SimpleOrderClause(this);
    }

    @NotNull
    @Override
    public Select skip(final int skip) {
        this.skip = skip;
        return this;
    }

    @Override
    public int getSkip() {
        return this.skip;
    }

    @NotNull
    @Override
    public Select limit(final int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public int getLimit() {
        return this.limit;
    }

    @Override
    public boolean hasSorting() {
        return !sorts.isEmpty();
    }

    @Nullable
    @Override
    public BitSet filteredUnlimited(
            @NotNull
            final IndexedDatabase database,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        if (conditions.isEmpty()) {
            return new ReadOnlyOneBitSet(database.getDocumentCount());
        } else {
            final Condition where = new SimpleAndCondition(conditions);
            final ArrayBitSet result =
                    bitSetPool.borrowSet(
                            database.getDocumentCount());
            if (where.set(database, result, bitSetPool)) {
                return result;
            } else {
                bitSetPool.returnSet(result);
                return null;
            }
        }
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
        assert !docs.isEmpty();

        // Shortcut if there is not sorting
        if (sorts.isEmpty()) {
            return new IdScoredDocumentIterator(database, docs);
        } else {
            return new SortingScoredDocumentIterator(database, docs, sorts);
        }
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @NotNull
    @Override
    public SimpleSelect clone() {
        return new SimpleSelect(
                new ArrayList<>(conditions),
                new ArrayList<>(sorts),
                skip,
                limit);
    }

    @Override
    public String toString() {
        return "SimpleSelect{" +
               "conditions=" + Joiner.on(',').join(conditions) +
               ", sorts=" + Joiner.on(',').join(sorts) +
               ", skip=" + skip +
               ", limit=" + limit +
               '}';
    }
}
