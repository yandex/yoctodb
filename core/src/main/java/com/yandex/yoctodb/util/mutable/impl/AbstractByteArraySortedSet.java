/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;

import java.util.*;

/**
 * {@link ByteArraySortedSet} abstract implementation with common methods
 *
 * @author incubos
 */
@NotThreadSafe
abstract class AbstractByteArraySortedSet
        implements ByteArraySortedSet {
    final SortedSet<UnsignedByteArray> elements;

    AbstractByteArraySortedSet(
            final SortedSet<UnsignedByteArray> elements) {
        this.elements = elements;
    }

    @Override
    public int indexOf(
            @NotNull
            final UnsignedByteArray e) {
        assert elements.contains(e) : "No such element";

        return elements.headSet(e).size();
    }
}
