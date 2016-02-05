/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import com.yandex.yoctodb.immutable.FilterableIndexProvider;
import com.yandex.yoctodb.query.BitSetPool;
import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.FilterableIndex;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.BitSet;

/**
 * Range condition
 *
 * @author incubos
 */
@Immutable
public final class SimpleRangeCondition extends AbstractTermCondition {
    @NotNull
    private final Buffer from;
    private final boolean fromInclusive;
    @NotNull
    private final Buffer to;
    private final boolean toInclusive;

    public SimpleRangeCondition(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray from,
            final boolean fromInclusive,
            @NotNull
            final UnsignedByteArray to,
            final boolean toInclusive) {
        super(fieldName);

        if (from.length() == 0)
            throw new IllegalArgumentException("Empty from value");
        if (to.length() == 0)
            throw new IllegalArgumentException("Empty to value");
        if (!(from.compareTo(to) < 0 ||
                from.equals(to) && fromInclusive && toInclusive))
            throw new IllegalArgumentException("Empty range");

        this.from = from.toByteBuffer();
        this.fromInclusive = fromInclusive;
        this.to = to.toByteBuffer();
        this.toInclusive = toInclusive;
    }

    @Override
    public boolean set(
            @NotNull
            final FilterableIndexProvider indexProvider,
            @NotNull
            final BitSet dest,
            @NotNull
            final BitSetPool bitSetPool) {
        final FilterableIndex index = indexProvider.getFilter(getFieldName());
        return index != null &&
                index.between(dest, from, fromInclusive, to, toInclusive);
    }
}
