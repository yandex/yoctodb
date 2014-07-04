/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.query;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.yandex.yoctodb.immutable.Database;
import ru.yandex.yoctodb.util.mutable.BitSet;

import java.util.Iterator;

/**
 * Query to be run against {@link Database}
 *
 * @author incubos
 */
@NotThreadSafe
public interface Query {
    /**
     * Calculate filtering result not taking into account skip/limit
     *
     * @param ctx query context
     * @return read-only filtering result or {@code null} if empty
     */
    @Nullable
    BitSet filteredUnlimited(
            @NotNull
            QueryContext ctx);

    /**
     * Return sorted results not taking into account skip/limit
     *
     * @param ctx query context
     * @return sorted results
     */
    @NotNull
    Iterator<? extends ScoredDocument<?>> sortedUnlimited(
            @NotNull
            BitSet docs,
            @NotNull
            QueryContext ctx);

    int getSkip();

    int getLimit();

    boolean hasSorting();
}
