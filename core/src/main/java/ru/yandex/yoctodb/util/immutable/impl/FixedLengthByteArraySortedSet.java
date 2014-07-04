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
import ru.yandex.yoctodb.util.UnsignedByteArrays;
import ru.yandex.yoctodb.util.immutable.ByteArraySortedSet;

import java.nio.ByteBuffer;

/**
 * {@link ru.yandex.yoctodb.util.immutable.ByteArraySortedSet} with fixed size
 * elements
 *
 * @author incubos
 */
@Immutable
public final class FixedLengthByteArraySortedSet
        extends AbstractByteArraySortedSet {
    private final int elementSize;
    private final int size;
    private final ByteBuffer elements;

    @NotNull
    public static ByteArraySortedSet from(
            @NotNull
            final ByteBuffer buf) {
        final int elementSize = buf.getInt();
        final int elementsCount = buf.getInt();

        return new FixedLengthByteArraySortedSet(
                elementSize,
                elementsCount,
                buf.slice());
    }

    private FixedLengthByteArraySortedSet(
            final int elementSize,
            final int elementsCount,
            final ByteBuffer elements) {
        assert elementSize > 0;
        assert elementsCount > 0;
        assert elements.hasRemaining();

        this.elementSize = elementSize;
        this.size = elementsCount;
        this.elements = elements;
    }

    @Override
    protected int compare(
            final int ith,
            @NotNull
            final ByteBuffer that) {
        assert 0 <= ith && ith < size;

        return UnsignedByteArrays.compare(
                elements,
                ith * elementSize,
                elementSize,
                that);
    }

    @Override
    public int size() {
        return size;
    }

    @NotNull
    @Override
    public ByteBuffer get(final int i) {
        assert 0 <= i && i < size;

        final ByteBuffer copy = elements.duplicate();
        copy.position(i * elementSize);
        copy.limit(copy.position() + elementSize);

        return copy.slice();
    }

    @Override
    public String toString() {
        return "FixedLengthByteArraySortedSet{" +
               "elementSize=" + elementSize +
               ", size=" + size +
               '}';
    }
}
