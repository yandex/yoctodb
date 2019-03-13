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

import com.google.common.primitives.Shorts;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * Fixed length immutable implementation of {@link ByteArrayIndexedList}
 *
 * @author incubos
 */
@Immutable
public class FixedLengthByteArrayIndexedList
        implements ByteArrayIndexedList {
    private final int elementSize;
    private final int elementCount;
    private final Buffer elements;

    @NotNull
    public static ByteArrayIndexedList from(
            @NotNull
            final Buffer buf) {
        final int elementSize = buf.getInt();
        final int elementCount = buf.getInt();

        return new FixedLengthByteArrayIndexedList(
                elementSize,
                elementCount,
                buf.slice());
    }

    private FixedLengthByteArrayIndexedList(
            final int elementSize,
            final int elementCount,
            final Buffer elements) {
        assert elementSize >= 0 : "Negative element size";
        assert elementCount >= 0 : "Negative element count";

        this.elementSize = elementSize;
        this.elementCount = elementCount;
        this.elements = elements;
    }

    @NotNull
    @Override
    public Buffer get(final int i) {
        assert 0 <= i && i < elementCount;

        return elements.slice(((long) i) * elementSize, elementSize);
    }

    @Override
    public long getLongUnsafe(final int i) {
        assert 0 <= i && i < elementCount;
        assert elementSize == Long.BYTES;

        return elements.getLong() ^ Long.MIN_VALUE;
    }

    @Override
    public int getIntUnsafe(final int i) {
        assert 0 <= i && i < elementCount;
        assert elementSize == Integer.BYTES;

        return elements.getInt() ^ Integer.MIN_VALUE;
    }

    @Override
    public short getShortUnsafe(final int i) {
        assert 0 <= i && i < elementCount;
        assert elementSize == Short.BYTES;

        final int res = elements.getShort() ^ Short.MIN_VALUE;

        return Shorts.checkedCast(res);
    }

    @Override
    public char getCharUnsafe(final int i) {
        assert 0 <= i && i < elementCount;
        assert elementSize == Character.BYTES;

        return elements.getChar();
    }

    @Override
    public byte getByteUnsafe(final int i) {
        assert 0 <= i && i < elementCount;
        assert elementSize == Byte.BYTES;

        final int res = elements.get() ^ Byte.MIN_VALUE;

        assert Byte.MIN_VALUE <= res && res <= Byte.MAX_VALUE;

        return (byte) res;
    }

    @Override
    public int size() {
        return elementCount;
    }

    @Override
    public String toString() {
        return "FixedLengthByteArrayIndexedList{" +
                "elementSize=" + elementSize +
                ", elementCount=" + elementCount +
                '}';
    }
}
