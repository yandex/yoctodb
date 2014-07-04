/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

/**
 * Select query
 *
 * @author incubos
 */
@NotThreadSafe
public interface Select extends Query {
    @NotNull
    Where where(
            @NotNull
            Condition condition);

    @NotNull
    OrderBy orderBy(
            @NotNull
            Order order);

    @NotNull
    Select skip(int skip);

    @NotNull
    Select limit(int limit);
}
