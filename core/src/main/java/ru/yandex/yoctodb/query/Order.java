/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query;

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
