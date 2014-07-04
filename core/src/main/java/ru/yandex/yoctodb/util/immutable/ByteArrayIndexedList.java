/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.immutable;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Indexed list of {@link ru.yandex.yoctodb.util.UnsignedByteArray}s
 *
 * @author incubos
 */
@ThreadSafe
public interface ByteArrayIndexedList {
    @NotNull
    ByteBuffer get(int i);

    int size();
}
