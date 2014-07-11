/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.query;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * {@link OrderBy} order
 *
 * @author incubos
 */
@Immutable
public interface Order {
    public static enum SortOrder {
        ASC,
        DESC
    }

    @NotNull
    String getFieldName();

    @NotNull
    SortOrder getOrder();
}
