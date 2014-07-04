/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * {@link ru.yandex.yoctodb.util.UnsignedByteArray} immutable sorted set with basic operations
 *
 * @author incubos
 */
@Immutable
public interface ByteArraySortedSet {
    int size();

    @NotNull
    ByteBuffer get(int i);

    /**
     * Get index of the element
     *
     * @param e the element to lookup
     *
     * @return index of the element or -1
     */
    int indexOf(
            @NotNull
            ByteBuffer e);

    /**
     * Index of element greater than {@code e} taking into account {@code
     * orEquals} parameter {@code upToIndexInclusive}
     *
     * @param e                  element to compare to
     * @param orEquals           inclusive flag
     * @param upToIndexInclusive right bound (inclusive)
     *
     * @return index of the first element or -1
     */
    int indexOfGreaterThan(
            @NotNull
            ByteBuffer e,
            boolean orEquals,
            int upToIndexInclusive);

    /**
     * Index of element less than {@code e} taking into account {@code orEquals}
     * parameter {@code fromIndexInclusive}
     *
     * @param e                  element to compare to
     * @param orEquals           inclusive flag
     * @param fromIndexInclusive left bound (inclusive)
     *
     * @return index of the first element or -1
     */
    int indexOfLessThan(
            @NotNull
            ByteBuffer e,
            boolean orEquals,
            int fromIndexInclusive);
}
