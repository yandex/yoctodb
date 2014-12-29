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

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.FilterableIndex;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.BitSet;

/**
 * Greater than or equals condition
 *
 * @author incubos
 */
@Immutable
public final class SimpleGreaterThanOrEqualsCondition
        extends AbstractSimpleCondition {
    @NotNull
    private final Buffer value;

    public SimpleGreaterThanOrEqualsCondition(
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
            final FilterableIndex index,
            @NotNull
            final BitSet to) {
        return index.greaterThan(to, value, true);
    }
}
