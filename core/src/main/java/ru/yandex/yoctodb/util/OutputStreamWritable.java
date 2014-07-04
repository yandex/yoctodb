/*
 * Copyright (c) 2014 Yandex
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
