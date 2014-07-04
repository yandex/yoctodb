/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.util;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Something writable to {@link OutputStream}
 *
 * @author incubos
 */
@ThreadSafe
public interface OutputStreamWritable {
    int getSizeInBytes();

    void writeTo(
            @NotNull
            OutputStream os) throws IOException;
}
