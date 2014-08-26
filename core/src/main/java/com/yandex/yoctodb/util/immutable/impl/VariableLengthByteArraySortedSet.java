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

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.immutable.ByteArraySortedSet;

import java.nio.ByteBuffer;

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
    private final ByteBuffer offsets;
    @NotNull
    private final ByteBuffer elements;

    public static ByteArraySortedSet from(
            @NotNull
            final ByteBuffer buffer) {
        final int maxElement = buffer.getInt();
        assert maxElement > 0;

        final int size = buffer.getInt();
        assert size > 0;

        final ByteBuffer offsets = buffer.slice();
        offsets.limit(4 * (size + 1));

        final ByteBuffer elements = buffer.slice();
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
            final ByteBuffer offsets,
            @NotNull
            final ByteBuffer elements) {
        assert maxElement > 0;
        assert size > 0;
        assert offsets.hasRemaining();
        assert elements.hasRemaining();

        this.maxElement = maxElement;
        this.size = size;
        this.offsets = offsets;
        this.elements = elements;
    }

    @Override
    protected int compare(
            final int ith,
            @NotNull
            final ByteBuffer that) {
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
    public ByteBuffer get(final int i) {
        assert 0 <= i && i < size;

        final int start = offsets.getInt(i << 2);
        final int end = offsets.getInt((i + 1) << 2);

        assert start < end;

        final ByteBuffer copy = elements.duplicate();
        copy.position(start);
        copy.limit(end);

        return copy.slice();
    }

    @Override
    public String toString() {
        return "VariableLengthByteArraySortedSet{" +
               "maxElement=" + maxElement +
               ", size=" + size +
               '}';
    }
}
