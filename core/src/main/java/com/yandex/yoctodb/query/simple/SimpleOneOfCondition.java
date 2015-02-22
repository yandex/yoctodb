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

import com.yandex.yoctodb.query.Condition;
import com.yandex.yoctodb.query.QueryContext;
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
public final class SimpleOneOfCondition implements Condition {
    @NotNull
    private final Collection<Condition> clauses;

    public SimpleOneOfCondition(
            @NotNull
            final Collection<Condition> conditions) {
        assert conditions.size() >= 2;

        this.clauses = new ArrayList<Condition>(conditions);
    }

    @Override
    public boolean set(
            @NotNull
            final QueryContext ctx,
            @NotNull
            final BitSet to) {
        boolean notEmpty = false;
        for (Condition clause : clauses)
            if (clause.set(ctx, to))
                notEmpty = true;

        return notEmpty;
    }
}
