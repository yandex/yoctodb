/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.OutputStreamWritable;

/**
 * {@link com.yandex.yoctodb.util.UnsignedByteArray} mutable sorted set with basic operations.
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
