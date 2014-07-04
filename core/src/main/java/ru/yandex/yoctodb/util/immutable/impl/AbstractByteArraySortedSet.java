/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.util.immutable.impl;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.immutable.ByteArraySortedSet;

import java.nio.ByteBuffer;

/**
 * Implementation of common binary search logic in {@link ByteArraySortedSet}
 *
 * @author incubos
 */
@Immutable
public abstract class AbstractByteArraySortedSet
        implements ByteArraySortedSet {

    protected abstract int compare(
            int ith,
            @NotNull
            ByteBuffer that);

    @Override
    public int indexOf(
            @NotNull
            final ByteBuffer e) {
        int start = 0;
        int end = size() - 1;
        while (start <= end) {
            final int mid = (start + end) >>> 1;
            final int compare = compare(mid, e);
            if (compare < 0) {
                start = mid + 1;
            } else if (compare > 0) {
                end = mid - 1;
            } else {
                return mid;
            }
        }

        return -1;
    }

    @Override
    public int indexOfGreaterThan(
            @NotNull
            final ByteBuffer e,
            final boolean orEquals,
            final int upToIndexInclusive) {
        assert 0 <= upToIndexInclusive && upToIndexInclusive < size();

        int start = 0;
        int end = upToIndexInclusive;
        while (start <= end) {
            final int mid = (start + end) >>> 1;
            final int compare = compare(mid, e);
            if (compare < 0) {
                start = mid + 1;
            } else if (compare > 0) {
                end = mid - 1;
            } else if (orEquals) {
                // Found equal
                return mid;
            } else if (mid == upToIndexInclusive) {
                // Equal is the last element, so not found not equal
                return -1;
            } else {
                // Equal is not the last element, so not equal is the next
                return mid + 1;
            }
        }

        if (start <= upToIndexInclusive) {
            return start;
        } else {
            return -1;
        }
    }

    @Override
    public int indexOfLessThan(
            @NotNull
            final ByteBuffer e,
            final boolean orEquals,
            final int fromIndexInclusive) {
        assert 0 <= fromIndexInclusive && fromIndexInclusive < size();

        int start = fromIndexInclusive;
        int end = size() - 1;
        while (start <= end) {
            final int mid = (start + end) >>> 1;
            final int compare = compare(mid, e);
            if (compare < 0) {
                start = mid + 1;
            } else if (compare > 0) {
                end = mid - 1;
            } else if (orEquals) {
                // Found equal
                return mid;
            } else if (mid == fromIndexInclusive) {
                // Equal is the first element, so not found not equal
                return -1;
            } else {
                // Equal is not the first element, so not equal is the previous
                return mid - 1;
            }
        }

        if (end >= fromIndexInclusive) {
            return end;
        } else {
            return -1;
        }
    }
}
