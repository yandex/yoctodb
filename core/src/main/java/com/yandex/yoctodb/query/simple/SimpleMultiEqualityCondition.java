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

import com.yandex.yoctodb.immutable.FilterableIndexProvider;
import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.FilterableIndex;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.BitSet;

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
    private final Buffer[] values;

    public SimpleMultiEqualityCondition(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray... values) {
        super(fieldName);

        if (values.length == 0)
            throw new IllegalArgumentException("Empty value array");

        final UnsignedByteArray[] sorted = values.clone();
        Arrays.sort(sorted);

        this.values = new Buffer[sorted.length];
        for (int i = 0; i < sorted.length; i++) {
            if (sorted[i].isEmpty())
                throw new IllegalArgumentException("Empty value");

            this.values[i] = sorted[i].toByteBuffer();
        }
    }

    @Override
    public boolean set(
            @NotNull
            final FilterableIndexProvider indexProvider,
            @NotNull
            final BitSet to) {
        final FilterableIndex index = indexProvider.getFilter(getFieldName());
        return index != null && index.in(to, values);
    }
}
