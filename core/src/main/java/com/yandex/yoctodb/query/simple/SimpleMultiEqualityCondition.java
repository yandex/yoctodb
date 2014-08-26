/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
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
