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

import com.yandex.yoctodb.immutable.FilterableIndexProvider;
import com.yandex.yoctodb.query.Condition;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;

/**
 * {@code OR} condition
 *
 * @author incubos
 */
@Immutable
public final class SimpleOrCondition implements Condition {
    @NotNull
    private final Iterable<Condition> clauses;

    public SimpleOrCondition(
            @NotNull
            final Collection<Condition> conditions) {
        if (conditions.isEmpty())
            throw new IllegalArgumentException("No conditions");

        this.clauses = new ArrayList<>(conditions);
    }

    @Override
    public boolean set(
            @NotNull
            final FilterableIndexProvider indexProvider,
            @NotNull
            final BitSet to,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        boolean notEmpty = false;
        for (Condition clause : clauses)
            if (clause.set(indexProvider, to, bitSetPool))
                notEmpty = true;

        return notEmpty;
    }
}
