/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query.simple;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.yandex.yoctodb.query.*;
import ru.yandex.yoctodb.util.mutable.BitSet;
import ru.yandex.yoctodb.util.mutable.impl.ReadOnlyOneBitSet;
import ru.yandex.yoctodb.util.mutable.impl.ReadOnlyZeroBitSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Simple {@link Select} implementation
 *
 * @author incubos
 */
@NotThreadSafe
public final class SimpleSelect implements Select {
    @NotNull
    private final List<AbstractSimpleCondition> conditions =
            new ArrayList<AbstractSimpleCondition>();
    @NotNull
    private final List<Order> sorts = new ArrayList<Order>();
    private int skip = 0;
    private int limit = Integer.MAX_VALUE;

    @NotNull
    @Override
    public Where where(
            @NotNull
            final Condition condition) {
        conditions.add((AbstractSimpleCondition) condition);
        return new SimpleWhereClause(this, conditions);
    }

    @NotNull
    @Override
    public OrderBy orderBy(
            @NotNull
            final Order order) {
        sorts.add(order);
        return new SimpleOrderClause(this, sorts);
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
            final AbstractSimpleCondition c = conditions.iterator().next();
            final BitSet result = ctx.getZeroBitSet();
            if (c.set(ctx.getFilter(c.getFieldName()), result)) {
                return result;
            } else {
                return null;
            }
        } else {
            // Searching
            final BitSet result = ctx.getOneBitSet();
            final BitSet conditionResult = ctx.getZeroBitSet();
            final Iterator<AbstractSimpleCondition> iter =
                    conditions.iterator();
            while (iter.hasNext()) {
                final AbstractSimpleCondition c = iter.next();
                if (!c.set(ctx.getFilter(c.getFieldName()), conditionResult)) {
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
               "conditions=" + conditions +
               ", sorts=" + sorts +
               ", skip=" + skip +
               ", limit=" + limit +
               '}';
    }
}
