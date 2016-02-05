/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query;

import com.yandex.yoctodb.query.simple.*;
import com.yandex.yoctodb.util.UnsignedByteArray;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

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

    // For test coverage
    static {
        new QueryBuilder();
    }

    @NotNull
    public static Select select() {
        return new SimpleSelect();
    }

    @NotNull
    public static TermCondition eq(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleEqualityCondition(fieldName, value);
    }

    @NotNull
    public static TermCondition lt(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleLessThanCondition(fieldName, value);
    }

    @NotNull
    public static TermCondition lte(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleLessThanOrEqualsCondition(fieldName, value);
    }

    @NotNull
    public static TermCondition gt(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleGreaterThanCondition(fieldName, value);
    }

    @NotNull
    public static TermCondition gte(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray value) {
        return new SimpleGreaterThanOrEqualsCondition(fieldName, value);
    }

    @NotNull
    public static TermCondition in(
            @NotNull
            final String fieldName,
            @NotNull
            final UnsignedByteArray... values) {
        return new SimpleMultiEqualityCondition(fieldName, values);
    }

    @NotNull
    public static TermCondition in(
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
    public static Condition not(
            @NotNull
            final Condition condition) {
        return new SimpleNotCondition(condition);
    }

    @NotNull
    private static Collection<Condition> collect(
            @NotNull
            final Condition c1,
            @NotNull
            final Condition c2,
            final Condition[] rest) {
        final Collection<Condition> conditions =
                new ArrayList<Condition>(rest.length + 2);

        conditions.add(c1);
        conditions.add(c2);

        if (rest.length > 0)
            conditions.addAll(Arrays.asList(rest));

        return conditions;
    }

    @NotNull
    public static Condition or(
            @NotNull
            final Condition c1,
            @NotNull
            final Condition c2,
            final Condition... rest) {
        return new SimpleOrCondition(collect(c1, c2, rest));
    }

    @NotNull
    public static Condition and(
            @NotNull
            final Condition c1,
            @NotNull
            final Condition c2,
            final Condition... rest) {
        return new SimpleAndCondition(collect(c1, c2, rest));
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
