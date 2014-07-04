/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.immutable.IntToIntArray;
import ru.yandex.yoctodb.util.mutable.BitSet;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Index sorting documents
 *
 * @author incubos
 */
@Immutable
public interface SortableIndex extends Index {
    /**
     * Get {@code document} sort value index
     *
     * @param document document
     * @return         index of sort value
     */
    int getSortValueIndex(int document);

    /**
     * Get sort value by index
     *
     * @param index sort value index
     * @return      sort value
     */
    @NotNull
    ByteBuffer getSortValue(int index);

    /**
     * Get ascending iterator for {@link IntToIntArray} values mapping
     * sort value index to possibly multiple documents
     *
     * @param docs bit set for filtered document
     * @return     ascending iterator
     */
    @NotNull
    Iterator<IntToIntArray> ascending(
            @NotNull
            BitSet docs);

    /**
     * Same as {@link #ascending(ru.yandex.yoctodb.util.mutable.BitSet)}
     *
     * @param docs bit set for filtered document
     * @return     descending iterator
     */
    @NotNull
    Iterator<IntToIntArray> descending(
            @NotNull
            BitSet docs);
}
