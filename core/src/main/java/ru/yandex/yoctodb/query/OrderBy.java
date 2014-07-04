/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

/**
 * Order by clause
 *
 * @author incubos
 */
@NotThreadSafe
public interface OrderBy extends Select {
    @NotNull
    OrderBy and(
            @NotNull
            Order order);
}
