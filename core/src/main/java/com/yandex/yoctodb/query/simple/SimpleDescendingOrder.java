/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.query.Order;

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
