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

import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.OutputStreamWritable;

/**
 * @author svyatoslav
 */
public interface TrieBasedByteArraySet extends OutputStreamWritable {
    /**
     * Adds element to this set
     *
     * @param e element to add
     * @return is element added to this set
     */
    UnsignedByteArray add(
            @NotNull
            UnsignedByteArray e);

    int indexOf(
            @NotNull
            UnsignedByteArray e);
}
