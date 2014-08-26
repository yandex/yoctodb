/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.mutable.BitSet;

import java.util.Iterator;

/**
 * @author svyatoslav
 *         Date: 21.11.13
 */
@Immutable
public interface IndexToIndexMultiMap {
    boolean get(
            @NotNull
            BitSet dest,
            int index);

    boolean getFrom(
            @NotNull
            BitSet dest,
            int fromInclusive);

    boolean getTo(
            @NotNull
            BitSet dest,
            int toExclusive);

    boolean getBetween(
            @NotNull
            BitSet dest,
            int fromInclusive,
            int toExclusive);

    @NotNull
    Iterator<IntToIntArray> ascending(
            @NotNull
            BitSet valueFilter);

    @NotNull
    Iterator<IntToIntArray> descending(
            @NotNull
            BitSet valueFilter);

    int getKeysCount();
}
