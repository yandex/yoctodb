/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.immutable.IntToIntArray;
import com.yandex.yoctodb.util.mutable.BitSet;

import java.util.Iterator;

/**
 * Index sorting documents
 *
 * @author incubos
 */
@Immutable
public interface SortableIndex extends StoredIndex {
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
    Buffer getSortValue(int index);

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
     * Same as {@link #ascending(com.yandex.yoctodb.util.mutable.BitSet)}
     *
     * @param docs bit set for filtered document
     * @return     descending iterator
     */
    @NotNull
    Iterator<IntToIntArray> descending(
            @NotNull
            BitSet docs);
}
