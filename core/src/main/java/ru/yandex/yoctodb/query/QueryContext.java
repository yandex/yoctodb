/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.query;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.immutable.Database;
import ru.yandex.yoctodb.immutable.FilterableIndex;
import ru.yandex.yoctodb.immutable.SortableIndex;
import ru.yandex.yoctodb.util.mutable.BitSet;

/**
 * Context for query execution
 *
 * @author incubos
 */
@Immutable
public interface QueryContext extends Database {
    @NotNull
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
