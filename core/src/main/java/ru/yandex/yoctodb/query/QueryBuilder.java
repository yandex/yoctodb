/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.query;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.query.simple.*;
import com.yandex.yoctodb.util.UnsignedByteArray;

/**
 * Abstract {@link Query} builder
 *
 * @author incubos
 */
@NotThreadSafe
public final class QueryBuilder {
    private QueryBuilder() {
        //
    }

    @NotNull
    public static Select select() {
        return new SimpleSelect();
    }

    @NotNull
    public static Condition eq(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleEqualityCondition(fieldName, value);
    }

    @NotNull
    public static Condition lt(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleLessThanCondition(fieldName, value);
    }

    @NotNull
    public static Condition lte(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleLessThanOrEqualsCondition(fieldName, value);
    }

    @NotNull
    public static Condition gt(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleGreaterThanCondition(fieldName, value);
    }

    @NotNull
    public static Condition gte(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleGreaterThanOrEqualsCondition(fieldName, value);
    }

    @NotNull
    public static Condition in(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray... values) {
        return new SimpleMultiEqualityCondition(fieldName, values);
    }

    @NotNull
    public static Condition in(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray from,
            final boolean fromInclusive,
            @NotNull
            final UnsignedByteArray to,
            final boolean toInclusive) {
        return new SimpleRangeCondition(
                fieldName,
                from,
                fromInclusive,
                to,
                toInclusive);
    }

    @NotNull
    public static Order asc(
            @NotNull
            final String fieldName) {
        return new SimpleAscendingOrder(fieldName);
    }

    @NotNull
    public static Order desc(
            @NotNull
            final String fieldName) {
        return new SimpleDescendingOrder(fieldName);
    }
}
