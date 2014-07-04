/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * Field index
 *
 * @author incubos
 */
@Immutable
public interface Index {
    @NotNull
    String getFieldName();
}
