/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
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
    private final int size;
    @NotNull
    private final Buffer offsets;
    @NotNull
    private final Buffer elements;

    public static ByteArraySortedSet from(
            @NotNull
            final Buffer buffer) {
        final int size = buffer.getInt();
        final Buffer offsets = buffer.slice(((long) (size + 1)) << 3);
        final Buffer elements = buffer.slice();
        elements.position(offsets.remaining());

        return new VariableLengthByteArraySortedSet(
                size,
                offsets.slice(),
                elements.slice());
    }

    private VariableLengthByteArraySortedSet(
            final int size,
            @NotNull
            final Buffer offsets,
            @NotNull
            final Buffer elements) {
        assert size >= 0 : "Negative size";

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

        final long base = ((long) ith) << 3;
        final long leftFrom = offsets.getLong(base);
        final long leftEnd = offsets.getLong(base + 8L);

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

        final long base = ((long) i) << 3;
        final long start = offsets.getLong(base);
        final long end = offsets.getLong(base + 8L);

        assert start < end;

        return elements.slice(start, end - start);
    }

    @Override
    public String toString() {
        return "VariableLengthByteArraySortedSet{" +
               "size=" + size +
               '}';
    }
}
