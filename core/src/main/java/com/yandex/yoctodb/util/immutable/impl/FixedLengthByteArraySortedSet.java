/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.google.common.primitives.Chars;
import com.google.common.primitives.Shorts;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

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
    public static FixedLengthByteArraySortedSet from(
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
        assert elementSize >= 0 : "Negative element size";
        assert elementCount >= 0 : "Negative element count";

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

        return elements.slice(((long) i) * elementSize, elementSize);
    }

    @Override
    public long getLongUnsafe(final int i) {
        assert 0 <= i && i < size;

        assert elementSize == Long.BYTES;

        return elements.getLong(((long) i) << 3) ^ Long.MIN_VALUE;
    }

    @Override
    public int getIntUnsafe(final int i) {
        assert 0 <= i && i < size;

        assert elementSize == Integer.BYTES;

        return elements.getInt(((long) i) << 2) ^ Integer.MIN_VALUE;
    }

    @Override
    public short getShortUnsafe(final int i) {
        assert 0 <= i && i < size;

        assert elementSize == Short.BYTES;

        final int res = elements.getShort(((long) i) << 1) ^ Short.MIN_VALUE;

        return Shorts.checkedCast(res);
    }

    @Override
    public char getCharUnsafe(final int i) {
        assert 0 <= i && i < size;

        assert elementSize == Chars.BYTES;

        return elements.getChar(((long) i) << 1);
    }

    @Override
    public byte getByteUnsafe(final int i) {
        assert 0 <= i && i < size;

        assert elementSize == Byte.BYTES;

        final int res = elements.get(((long) i)) ^ Byte.MIN_VALUE;

        assert Byte.MIN_VALUE <= res && res <= Byte.MAX_VALUE;

        return (byte) res;
    }

    @Override
    public String toString() {
        return "FixedLengthByteArraySortedSet{" +
               "elementSize=" + elementSize +
               ", size=" + size +
               '}';
    }
}
