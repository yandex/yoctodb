/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable.impl;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.mutable.ByteArraySortedSet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

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
        assert e.length() > 0;

        if (frozen)
            throw new IllegalStateException("The collection is frozen");

        final UnsignedByteArray previous = elements.get(e);
        if (previous == null) {
            elements.put(e, e);

            return e;
        } else {
            return previous;
        }
    }

    protected void build() {
        assert !frozen;

        // Sorting
        final UnsignedByteArray[] sorted =
                elements.keySet().toArray(new UnsignedByteArray[elements.size()]);
        Arrays.sort(sorted);

        // Releasing resources
        elements = null;

        // Copying
        sortedElements = new LinkedHashMap<UnsignedByteArray, Integer>(sorted.length);
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

        assert result != null;

        return result;
    }

    @Override
    public int size() {
        if (elements != null) {
            return elements.size();
        }

        if (sortedElements != null) {
            return sortedElements.size();
        }
        return elements.size();
    }
}
