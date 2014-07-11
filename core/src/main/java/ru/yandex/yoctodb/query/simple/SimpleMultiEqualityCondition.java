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
import java.util.Arrays;

/**
 * {@code in} condition
 *
 * @author incubos
 */
@Immutable
public final class SimpleMultiEqualityCondition
        extends AbstractSimpleCondition {
    @NotNull
    private final ByteBuffer[] values;

    public SimpleMultiEqualityCondition(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray... values) {
        super(fieldName);

        assert values.length > 0;

        final UnsignedByteArray[] sorted = values.clone();
        Arrays.sort(sorted);

        this.values = new ByteBuffer[sorted.length];
        for (int i = 0; i < sorted.length; i++)
            this.values[i] = sorted[i].toByteBuffer();
    }

    @Override
    public boolean set(
            @NotNull
            final FilterableIndex index,
            @NotNull
            final BitSet to) {
        return index.in(to, values);
    }
}
