/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

/**
 * {@link BitSet} pool
 *
 * @author incubos
 */
@ThreadSafe
public interface ArrayBitSetPool {
    /**
     * Borrow a zeroed {@link ArrayBitSet} instance.
     *
     * @param size size of {@link ArrayBitSet}
     * @return An instance of {@link ArrayBitSet}
     */
    @NotNull
    ArrayBitSet borrowSet(int size);

    void returnSet(
            @NotNull
            ArrayBitSet set);
}
