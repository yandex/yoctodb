/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query.simple;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.immutable.FilterableIndex;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.mutable.BitSet;

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
