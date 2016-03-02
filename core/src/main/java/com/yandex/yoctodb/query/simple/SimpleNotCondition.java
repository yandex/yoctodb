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
import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
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
    private final Condition delegate;

    public SimpleNotCondition(
            @NotNull
            final Condition delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean set(
            @NotNull
            final FilterableIndexProvider indexProvider,
            @NotNull
            final BitSet to,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        final ArrayBitSet result = bitSetPool.borrowSet(to.getSize());
        try {
            delegate.set(indexProvider, result, bitSetPool);
            result.inverse();
            return to.or(result);
        } finally {
            bitSetPool.returnSet(result);
        }
    }
}
