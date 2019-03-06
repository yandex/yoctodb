/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * {@link com.yandex.yoctodb.util.UnsignedByteArray} immutable sorted set with basic operations
 *
 * @author incubos
 */
@Immutable
public interface ByteArraySortedSet {
    int size();

    @NotNull
    Buffer get(int i);

    long getLongUnsafe(int i);

    int getIntUnsafe(int i);

    short getShortUnsafe(int i);

    char getCharUnsafe(int i);

    byte getByteUnsafe(int i);

    /**
     * Get index of the element
     *
     * @param e the element to lookup
     *
     * @return index of the element or -1
     */
    int indexOf(
            @NotNull
            Buffer e);

    /**
     * Index of element greater than {@code e} taking into account {@code
     * orEquals} parameter {@code upToIndexInclusive}
     *
     * @param e                  element to compare to
     * @param orEquals           inclusive flag
     * @param upToIndexInclusive right bound (inclusive)
     *
     * @return index of the first element or -1
     */
    int indexOfGreaterThan(
            @NotNull
            Buffer e,
            boolean orEquals,
            int upToIndexInclusive);

    /**
     * Index of element less than {@code e} taking into account {@code orEquals}
     * parameter {@code fromIndexInclusive}
     *
     * @param e                  element to compare to
     * @param orEquals           inclusive flag
     * @param fromIndexInclusive left bound (inclusive)
     *
     * @return index of the first element or -1
     */
    int indexOfLessThan(
            @NotNull
            Buffer e,
            boolean orEquals,
            int fromIndexInclusive);
}
