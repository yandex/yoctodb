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

import com.yandex.yoctodb.immutable.IndexedDatabase;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.util.mutable.BitSet;

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
     * @param database      database
     * @param bitSetPool {@link BitSet} factory
     * @return read-only filtering result or {@code null} if empty
     */
    @Nullable
    BitSet filteredUnlimited(
            @NotNull
            IndexedDatabase database,
            @NotNull
            BitSetPool bitSetPool);

    /**
     * Return sorted results not taking into account skip/limit
     *
     * @param database      database
     * @param bitSetPool {@link BitSet} factory
     * @return sorted results
     */
    @NotNull
    Iterator<? extends ScoredDocument<?>> sortedUnlimited(
            @NotNull
            BitSet docs,
            @NotNull
            IndexedDatabase database,
            @NotNull
            BitSetPool bitSetPool);

    int getSkip();

    int getLimit();

    boolean hasSorting();
}
