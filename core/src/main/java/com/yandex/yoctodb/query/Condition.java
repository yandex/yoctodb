/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query;

import com.yandex.yoctodb.immutable.FilterableIndexProvider;
import com.yandex.yoctodb.util.mutable.BitSet;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * {@link Where} condition
 *
 * @author incubos
 */
@Immutable
public interface Condition {
    /**
     * Set bits for the documents satisfying condition and leave all the other
     * bits untouched.
     *
     * {@code false} return value is used for short-circuiting, so it is safe
     * to return {@code true}, but not otherwise.
     *
     * @param indexProvider index provider
     * @param to            container of filtered documents
     * @param bitSetPool    temporary bit set pool
     * @return if at least one bit was set
     */
    boolean set(
            @NotNull
            FilterableIndexProvider indexProvider,
            @NotNull
            BitSet to,
            @NotNull
            BitSetPool bitSetPool);
}
