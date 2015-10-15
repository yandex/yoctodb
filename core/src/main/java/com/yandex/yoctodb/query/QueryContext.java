/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.immutable.FilterableIndex;
import com.yandex.yoctodb.immutable.SortableIndex;
import com.yandex.yoctodb.util.mutable.BitSet;
import org.jetbrains.annotations.Nullable;

/**
 * Context for query execution
 *
 * @author incubos
 */
@Immutable
public interface QueryContext extends Database {
    @Nullable
    FilterableIndex getFilter(
            @NotNull
            String fieldName);

    @NotNull
    SortableIndex getSorter(
            @NotNull
            String fieldName);

    /**
     * You can borrow it only once per query
     *
     * @return {@link BitSet} instance with all the bits unset
     */
    @NotNull
    BitSet getZeroBitSet();

    /**
     * You can borrow it only once per query
     *
     * @return {@link BitSet} instance with all the bits set
     */
    @NotNull
    BitSet getOneBitSet();
}
