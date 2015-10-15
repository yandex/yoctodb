/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
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
    protected Map<UnsignedByteArray, UnsignedByteArray> elements =
            new HashMap<UnsignedByteArray, UnsignedByteArray>();
    protected boolean frozen = false;
    protected Map<UnsignedByteArray, Integer> sortedElements = null;

    @NotNull
    @Override
    public UnsignedByteArray add(
            @NotNull
            final UnsignedByteArray e) {
        if (frozen)
            throw new IllegalStateException("The collection is frozen");

        if (e.isEmpty())
            throw new IllegalArgumentException("Empty element");

        final UnsignedByteArray previous = elements.get(e);
        if (previous == null) {
            elements.put(e, e);

            return e;
        } else {
            return previous;
        }
    }

    protected void build() {
        if (frozen)
            throw new IllegalStateException("The collection is frozen");

        // Sorting
        final UnsignedByteArray[] sorted =
                elements.keySet().toArray(
                        new UnsignedByteArray[elements.size()]);
        Arrays.sort(sorted);

        // Releasing resources
        elements = null;

        // Copying
        sortedElements =
                new LinkedHashMap<UnsignedByteArray, Integer>(sorted.length);
        int i = 0;
        for (UnsignedByteArray e : sorted) {
            sortedElements.put(e, i++);
        }

        // Freezing
        frozen = true;
    }

    @Override
    public int indexOf(
            @NotNull
            final UnsignedByteArray e) {
        if (!frozen)
            build();

        final Integer result = sortedElements.get(e);

        if (result == null)
            throw new NoSuchElementException();

        return result;
    }

    @Override
    public int size() {
        if (frozen)
            return sortedElements.size();
        else
            return elements.size();
    }
}
