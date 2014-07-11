/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
