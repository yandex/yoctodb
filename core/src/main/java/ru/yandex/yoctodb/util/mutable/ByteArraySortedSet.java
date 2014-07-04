/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.OutputStreamWritable;

/**
 * {@link ru.yandex.yoctodb.util.UnsignedByteArray} mutable sorted set with basic operations.
 *
 * The contract is to {@code add} some elements and then call
 * {@code indexOf} to extract their indexes.
 *
 * @author incubos
 */
@NotThreadSafe
public interface ByteArraySortedSet extends OutputStreamWritable {
    /**
     * Adds element to this set
     *
     * @param e element to add
     *
     * @return element added or existing element
     */
    @NotNull
    UnsignedByteArray add(
            @NotNull
            UnsignedByteArray e);

    int indexOf(
            @NotNull
            UnsignedByteArray e);

    int size();
}
