/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
