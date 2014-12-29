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
 * {@link com.yandex.yoctodb.util.immutable.ByteArraySortedSet} with variable
 * sized elements
 *
 * @author incubos
 */
@Immutable
public final class VariableLengthByteArraySortedSet
        extends AbstractByteArraySortedSet {
    private final int maxElement;
    private final int size;
    @NotNull
    private final Buffer offsets;
    @NotNull
    private final Buffer elements;

    public static ByteArraySortedSet from(
            @NotNull
            final Buffer buffer) {
        final int maxElement = buffer.getInt();
        final int size = buffer.getInt();
        final Buffer offsets = buffer.slice((size + 1) << 2);
        final Buffer elements = buffer.slice();
        elements.position(offsets.remaining());

        return new VariableLengthByteArraySortedSet(
                maxElement,
                size,
                offsets.slice(),
                elements.slice());
    }

    private VariableLengthByteArraySortedSet(
            final int maxElement,
            final int size,
            @NotNull
            final Buffer offsets,
            @NotNull
            final Buffer elements) {
        if (maxElement <= 0)
            throw new IllegalArgumentException("Non positive max element");
        if (size <= 0)
            throw new IllegalArgumentException("Non positive size");
        if (!offsets.hasRemaining())
            throw new IllegalArgumentException("Empty offsets");
        if (!elements.hasRemaining())
            throw new IllegalArgumentException("Empty elements");

        this.maxElement = maxElement;
        this.size = size;
        this.offsets = offsets;
        this.elements = elements;
    }

    @Override
    protected int compare(
            final int ith,
            @NotNull
            final Buffer that) {
        assert 0 <= ith && ith < size;

        final int leftFrom = offsets.getInt(ith << 2);
        final int leftEnd = offsets.getInt((ith + 1) << 2);

        assert leftFrom < leftEnd;

        return UnsignedByteArrays.compare(
                elements,
                leftFrom,
                leftEnd - leftFrom,
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

        final int start = offsets.getInt(i << 2);
        final int end = offsets.getInt((i + 1) << 2);

        assert start < end;

        return elements.slice(start, end - start);
    }

    @Override
    public String toString() {
        return "VariableLengthByteArraySortedSet{" +
               "maxElement=" + maxElement +
               ", size=" + size +
               '}';
    }
}
