/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.OutputStreamWritable;

import java.util.NoSuchElementException;

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

    /**
     * Returns index of the element
     *
     * @param e element
     * @return  index of the element or {@link NoSuchElementException}
     */
    int indexOf(
            @NotNull
            UnsignedByteArray e) throws NoSuchElementException;
}
