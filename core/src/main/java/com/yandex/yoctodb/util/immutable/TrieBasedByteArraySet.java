/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

/**
 * @author svyatoslav
 */
public interface TrieBasedByteArraySet {
    int size();

    /**
     * Get index of the element
     *
     * @param e the element to lookup
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
     * @return index of the first element or -1
     */
    int indexOfLessThan(
            @NotNull
            Buffer e,
            boolean orEquals,
            int fromIndexInclusive);

}
