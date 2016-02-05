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
import com.yandex.yoctodb.query.BitSetPool;
import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.FilterableIndex;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.BitSet;

/**
 * Greater than condition
 *
 * @author incubos
 */
@Immutable
public final class SimpleGreaterThanCondition extends AbstractTermCondition {
    @NotNull
    private final Buffer value;

    public SimpleGreaterThanCondition(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        super(fieldName);

        if (value.length() == 0)
            throw new IllegalArgumentException("Empty value");

        this.value = value.toByteBuffer();
    }

    @Override
    public boolean set(
            @NotNull
            final FilterableIndexProvider indexProvider,
            @NotNull
            final BitSet to,
            @NotNull
            final BitSetPool bitSetPool) {
        final FilterableIndex index = indexProvider.getFilter(getFieldName());
        return index != null && index.greaterThan(to, value, false);
    }
}
