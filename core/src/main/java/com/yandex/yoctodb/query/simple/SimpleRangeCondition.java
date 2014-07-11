/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.query.simple;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.FilterableIndex;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.BitSet;

import java.nio.ByteBuffer;

/**
 * Range condition
 *
 * @author incubos
 */
@Immutable
public final class SimpleRangeCondition extends AbstractSimpleCondition {
    @NotNull
    private final ByteBuffer from;
    private final boolean fromInclusive;
    @NotNull
    private final ByteBuffer to;
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

        assert from.length() > 0;
        assert to.length() > 0;
        assert from.compareTo(to) < 0;

        this.from = from.toByteBuffer();
        this.fromInclusive = fromInclusive;
        this.to = to.toByteBuffer();
        this.toInclusive = toInclusive;
    }

    @Override
    public boolean set(
            @NotNull
            final FilterableIndex index,
            @NotNull
            final BitSet dest) {
        return index.between(dest, from, fromInclusive, to, toInclusive);
    }
}
