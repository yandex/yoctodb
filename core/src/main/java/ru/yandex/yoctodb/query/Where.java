/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

/**
 * Filtering clause
 *
 * @author incubos
 */
@NotThreadSafe
public interface Where extends Select {
    @NotNull
    Where and(
            @NotNull
            Condition condition);
}
