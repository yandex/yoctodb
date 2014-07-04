/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.OutputStreamWritable;

/**
 * Indexed list of {@link ru.yandex.yoctodb.util.UnsignedByteArray}s
 *
 * @author incubos
 */
@NotThreadSafe
public interface ByteArrayIndexedList extends OutputStreamWritable {
    void add(
            @NotNull
            UnsignedByteArray e);
}
