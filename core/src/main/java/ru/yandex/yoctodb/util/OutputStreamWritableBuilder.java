/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

/**
 * Builds {@link OutputStreamWritable}
 *
 * @author incubos
 */
@NotThreadSafe
public interface OutputStreamWritableBuilder {
    @NotNull
    OutputStreamWritable buildWritable();
}
