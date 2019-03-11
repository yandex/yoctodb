/*
 * (C) YANDEX LLC, 2014-2019
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * {@link com.yandex.yoctodb.util.immutable.ByteArrayIndexedList} with fixed size
 * elements
 *
 * @author irenkamalova
 */
@Immutable
public final class FoldedByteArrayIndexedList implements ByteArrayIndexedList {
    private final int elementCount;
    @NotNull
    private final Function<Integer, Integer> getOffsetIndex;
    @NotNull
    private final Buffer elements;
    @NotNull
    private final Buffer offsets;
    @NotNull
    private final Buffer indexes;

    @NotNull
    public static ByteArrayIndexedList from(
            @NotNull final Buffer buf) {
        final int elementsCount = buf.getInt();
        final int offsetsCount = buf.getInt();

        final int sizeOfIndexOffsetValue;
        if (offsetsCount < (1 << Byte.SIZE)) {
            sizeOfIndexOffsetValue = Byte.BYTES;
        } else if (offsetsCount < (1 << Short.SIZE)) {
            sizeOfIndexOffsetValue = Short.BYTES;
        } else {
            sizeOfIndexOffsetValue = Integer.BYTES;
        }
        // indexes of offsets
        final Buffer indexes = buf.slice((elementsCount) * sizeOfIndexOffsetValue);
        buf.position(buf.position() + indexes.remaining());

        // then offsets of element value
        final long offsetsSizeBytes = offsetsCount * Long.BYTES;
        final Buffer offsets = buf.slice(offsetsSizeBytes);
        buf.position(buf.position() + offsets.remaining());

        // then elements value
        final Buffer elements = buf.slice();

        return new FoldedByteArrayIndexedList(
                elementsCount,
                sizeOfIndexOffsetValue,
                indexes,
                offsets,
                elements);
    }

    private FoldedByteArrayIndexedList(
            final int elementCount,
            final int sizeOfIndexOffsetValue,
            @NotNull final Buffer indexes,
            @NotNull final Buffer offsets,
            @NotNull final Buffer elements) {
        assert elementCount >= 0 : "Negative element count";

        this.elementCount = elementCount;
        this.indexes = indexes;
        this.elements = elements;
        this.offsets = offsets;

        if (sizeOfIndexOffsetValue == Byte.BYTES) {
            this.getOffsetIndex = this::oneByteToInt;
        } else if (sizeOfIndexOffsetValue == Short.BYTES) {
            this.getOffsetIndex = this::twoBytesToInt;
        } else {
            this.getOffsetIndex = this::fourBytesToInt;
        }
    }

    @NotNull
    @Override
    public Buffer get(final int docId) {
        assert 0 <= docId && docId < elementCount;

        final long offsetIndex = getOffsetIndex.apply(docId) * Long.BYTES;
        final long start = offsets.getLong(offsetIndex);
        final long end = offsets.getLong(offsetIndex + Long.BYTES);
        return elements.slice(start, end - start);
    }

    @Override
    public long getLongUnsafe(final int docId) {
        assert 0 <= docId && docId < elementCount;

        final long offsetIndex = getOffsetIndex.apply(docId) * Long.BYTES;
        final long start = offsets.getLong(offsetIndex);

        return elements.getLong(start) ^ Long.MIN_VALUE;
    }

    @Override
    public int getIntUnsafe(final int docId) {
        assert 0 <= docId && docId < elementCount;

        final long offsetIndex = getOffsetIndex.apply(docId) * Long.BYTES;
        final long start = offsets.getLong(offsetIndex);

        return elements.getInt(start) ^ Integer.MIN_VALUE;
    }

    @Override
    public short getShortUnsafe(final int docId) {
        assert 0 <= docId && docId < elementCount;

        final long offsetIndex = getOffsetIndex.apply(docId) * Long.BYTES;
        final long start = offsets.getLong(offsetIndex);

        return (short) (elements.getShort(start) ^ Short.MIN_VALUE);
    }

    @Override
    public char getCharUnsafe(final int docId) {
        assert 0 <= docId && docId < elementCount;

        final long offsetIndex = getOffsetIndex.apply(docId) * Long.BYTES;
        final long start = offsets.getLong(offsetIndex);

        return elements.getChar(start);
    }

    @Override
    public byte getByteUnsafe(final int docId) {
        assert 0 <= docId && docId < elementCount;

        final long offsetIndex = getOffsetIndex.apply(docId) * Long.BYTES;
        final long start = offsets.getLong(offsetIndex);

        return (byte) (elements.get(start) ^ Byte.MIN_VALUE);
    }

    @Override
    public int size() {
        return elementCount;
    }

    @Override
    public String toString() {
        return "FoldedByteArrayIndexedList{" +
                "elementCount=" + elementCount +
                '}';
    }

    private int oneByteToInt(int docId) {
        return (Byte.toUnsignedInt(indexes.get(docId)));
    }

    private int twoBytesToInt(int docId) {
        return (Short.toUnsignedInt(
                indexes.getShort(docId
                        * Short.BYTES)));
    }

    private int fourBytesToInt(int docId) {
        return indexes.getInt(docId
                * Integer.BYTES);
    }
}
