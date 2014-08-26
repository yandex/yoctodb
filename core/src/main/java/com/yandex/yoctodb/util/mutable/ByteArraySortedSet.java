/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
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
