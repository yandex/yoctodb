/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query.simple;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.query.Order;

/**
 * Ascending order
 *
 * @author incubos
 */
@Immutable
public final class SimpleAscendingOrder implements Order {
    @NotNull
    private final String fieldName;

    public SimpleAscendingOrder(
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
        return SortOrder.ASC;
    }

    @Override
    public String toString() {
        return "SimpleAscendingOrder{" +
                "fieldName='" + fieldName + '\'' +
                '}';
    }
}
