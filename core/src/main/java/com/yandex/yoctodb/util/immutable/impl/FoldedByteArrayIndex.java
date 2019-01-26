package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import org.jetbrains.annotations.NotNull;

public class FoldedByteArrayIndex implements ByteArrayIndexedList {
    private final int elementCount;
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

        // indexes of offsets
        final Buffer indexes = buf.slice((elementsCount) << 2);

        long shift = indexes.remaining();

        final int offsetsCount = buf.slice()
                .position(shift)
                .slice().getInt();

        shift = shift + 4;

        // then offsets of element value
        final Buffer offsets = buf.slice()
                .position(shift)
                .slice((offsetsCount) << 3);

        shift = shift + offsets.remaining();

        // then elements value
        final Buffer elements = buf.slice()
                .position(shift)
                .slice();

        return new FoldedByteArrayIndex(
                elementsCount,
                offsets,
                elements,
                indexes);
    }

    private FoldedByteArrayIndex(
            final int elementCount,
            @NotNull final Buffer offsets,
            @NotNull final Buffer elements,
            @NotNull final Buffer indexes) {
        assert elementCount >= 0 : "Negative element count";

        this.elementCount = elementCount;
        this.elements = elements;
        this.offsets = offsets;
        this.indexes = indexes;
    }

    @NotNull
    @Override
    public Buffer get(final int docId) {
        assert 0 <= docId && docId < elementCount;

        int offsetIndex = indexes.getInt(((long) docId) << 2);
        long start = offsets.getLong(offsetIndex << 3);
        long end = offsets.getLong((offsetIndex + 1) << 3);
        return elements.slice(start, end - start);
    }

    @Override
    public int size() {
        return elementCount;
    }

    @Override
    public String toString() {
        return "FoldedByteArrayIndex{" +
                "elementCount=" + elementCount +
                '}';
    }
}
