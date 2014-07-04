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
import ru.yandex.yoctodb.immutable.FilterableIndex;
import ru.yandex.yoctodb.query.Condition;
import ru.yandex.yoctodb.util.mutable.BitSet;

/**
 * Abstract condition
 *
 * @author incubos
 */
@Immutable
abstract class AbstractSimpleCondition implements Condition {
    @NotNull
    private final String fieldName;

    AbstractSimpleCondition(
            @NotNull
            final String fieldName) {
        assert !fieldName.isEmpty();

        this.fieldName = fieldName;
    }

    @NotNull
    public String getFieldName() {
        return fieldName;
    }

    public abstract boolean set(
            @NotNull
            final FilterableIndex index,
            @NotNull
            final BitSet to);

    @Override
    public String toString() {
        return "AbstractSimpleCondition{" +
                "fieldName='" + fieldName + '\'' +
                '}';
    }
}
