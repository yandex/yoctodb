/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.query.simple;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.query.Order;

/**
 * Descending order
 *
 * @author incubos
 */
@Immutable
public final class SimpleDescendingOrder implements Order {
    @NotNull
    private final String fieldName;

    public SimpleDescendingOrder(
            @NotNull
            final String fieldName) {
        assert !fieldName.isEmpty();

        this.fieldName = fieldName;
    }

    @Override
    @NotNull
    public String getFieldName() {
        return fieldName;
    }

    @NotNull
    @Override
    public SortOrder getOrder() {
        return SortOrder.DESC;
    }

    @Override
    public String toString() {
        return "SimpleDescendingOrder{" +
                "fieldName='" + fieldName + '\'' +
                '}';
    }
}
