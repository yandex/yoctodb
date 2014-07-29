/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
