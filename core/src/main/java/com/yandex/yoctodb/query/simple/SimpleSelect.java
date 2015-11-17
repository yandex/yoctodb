/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import com.google.common.base.Joiner;
import com.yandex.yoctodb.query.*;
import com.yandex.yoctodb.util.mutable.BitSet;
import com.yandex.yoctodb.util.mutable.impl.ReadOnlyOneBitSet;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    private final List<Condition> conditions = new LinkedList<Condition>();
    @NotNull
    private final List<Order> sorts = new LinkedList<Order>();
    private int skip = 0;
    private int limit = Integer.MAX_VALUE;

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
            final QueryContext ctx) {
        if (conditions.isEmpty()) {
            return new ReadOnlyOneBitSet(ctx.getDocumentCount());
        } else if (conditions.size() == 1) {
            final Condition c = conditions.iterator().next();
            final BitSet result = ctx.getZeroBitSet();
            if (c.set(ctx, result)) {
                return result;
            } else {
                return null;
            }
        } else {
            // Searching
            final BitSet result = ctx.getOneBitSet();
            final BitSet conditionResult = ctx.getZeroBitSet();
            final Iterator<Condition> iter = conditions.iterator();
            while (iter.hasNext()) {
                final Condition c = iter.next();
                if (!c.set(ctx, conditionResult)) {
                    return null;
                }
                if (!result.and(conditionResult)) {
                    return null;
                }
                if (iter.hasNext()) {
                    conditionResult.clear();
                }
            }

            assert !result.isEmpty();

            return result;
        }
    }

    @NotNull
    @Override
    public Iterator<? extends ScoredDocument<?>> sortedUnlimited(
            @NotNull
            final BitSet docs,
            @NotNull
            final QueryContext ctx) {
        assert !docs.isEmpty();

        // Shortcut if there is not sorting
        if (sorts.isEmpty()) {
            return new IdScoredDocumentIterator(ctx, docs);
        } else {
            return new SortingScoredDocumentIterator(ctx, docs, sorts);
        }
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
