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

/**
 * Array-based {@link BitSet} implementation
 *
 * @author incubos
 */
public interface ArrayBitSet extends BitSet {
    @NotNull
    long[] toArray();
}
