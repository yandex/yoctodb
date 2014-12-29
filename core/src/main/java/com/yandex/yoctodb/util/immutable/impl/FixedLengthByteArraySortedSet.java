/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.immutable.ByteArraySortedSet;

/**
 * {@link com.yandex.yoctodb.util.immutable.ByteArraySortedSet} with fixed size
 * elements
 *
 * @author incubos
 */
@Immutable
public final class FixedLengthByteArraySortedSet
        extends AbstractByteArraySortedSet {
    private final int elementSize;
    private final int size;
    private final Buffer elements;

    @NotNull
    public static ByteArraySortedSet from(
            @NotNull
            final Buffer buf) {
        final int elementSize = buf.getInt();
        final int elementsCount = buf.getInt();

        return new FixedLengthByteArraySortedSet(
                elementSize,
                elementsCount,
                buf.slice());
    }

    private FixedLengthByteArraySortedSet(
            final int elementSize,
            final int elementCount,
            final Buffer elements) {
        if (elementSize <= 0)
            throw new IllegalArgumentException("Non positive element size");
        if (elementCount <= 0)
            throw new IllegalArgumentException("Non positive element count");
        if (!elements.hasRemaining())
            throw new IllegalArgumentException("Empty elements");

        this.elementSize = elementSize;
        this.size = elementCount;
        this.elements = elements;
    }

    @Override
    protected int compare(
            final int ith,
            @NotNull
            final Buffer that) {
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
    public Buffer get(final int i) {
        assert 0 <= i && i < size;

        return elements.slice(i * elementSize, elementSize);
    }

    @Override
    public String toString() {
        return "FixedLengthByteArraySortedSet{" +
               "elementSize=" + elementSize +
               ", size=" + size +
               '}';
    }
}
