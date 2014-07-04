/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
