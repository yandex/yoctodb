/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import com.yandex.yoctodb.query.TermCondition;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract condition
 *
 * @author incubos
 */
@Immutable
abstract class AbstractSimpleCondition implements TermCondition {
    @NotNull
    private final String fieldName;

    AbstractSimpleCondition(
            @NotNull
            final String fieldName) {
        if (fieldName.isEmpty())
            throw new IllegalArgumentException("Empty field name");

        this.fieldName = fieldName;
    }

    @NotNull
    protected String getFieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() +
               "{fieldName='" + fieldName + '\'' +
               '}';
    }
}
