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
import com.yandex.yoctodb.query.TermCondition;
import com.yandex.yoctodb.util.mutable.BitSet;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * Condition negation
 *
 * @author incubos
 */
@Immutable
public final class SimpleNotCondition implements Condition {
    @NotNull
    private final TermCondition delegate;

    public SimpleNotCondition(
            @NotNull
            final TermCondition delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean set(
            @NotNull
            final QueryContext ctx,
            @NotNull
            final BitSet to) {
        delegate.set(ctx, to);
        return to.inverse();
    }
}
