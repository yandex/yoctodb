package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public class FoldedByteArrayIndex implements ByteArrayIndexedList {
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

        int sizeOfIndexOffsetValue;
        if (offsetsCount < (1 << Byte.SIZE)) {
            sizeOfIndexOffsetValue = Byte.BYTES;
        } else if (offsetsCount < (1 << Short.SIZE)) {
            sizeOfIndexOffsetValue = Short.BYTES;
        } else {
            sizeOfIndexOffsetValue = Integer.BYTES;
        }

        // indexes of offsets
        final Buffer indexes = buf.slice((elementsCount) * sizeOfIndexOffsetValue);

        long shift = indexes.remaining();

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
                sizeOfIndexOffsetValue,
                offsets,
                elements,
                indexes);
    }

    private FoldedByteArrayIndex(
            final int elementCount,
            final int sizeOfIndexOffsetValue,
            @NotNull final Buffer offsets,
            @NotNull final Buffer elements,
            @NotNull final Buffer indexes) {
        assert elementCount >= 0 : "Negative element count";

        this.elementCount = elementCount;
        this.elements = elements;
        this.offsets = offsets;
        this.indexes = indexes;

        if (sizeOfIndexOffsetValue == Byte.BYTES) { // one byte 2^8 - 1 = 127
            this.getOffsetIndex = this::oneByteToInt;
        } else if (sizeOfIndexOffsetValue == Short.BYTES) {  // to  2^16 - 1 = 65535
            this.getOffsetIndex = this::twoBytesToInt;
        } else {
            this.getOffsetIndex = this::fourBytesToInt;
        }
    }

    @NotNull
    @Override
    public Buffer get(final int docId) {
        assert 0 <= docId && docId < elementCount;

        int offsetIndex = getOffsetIndex.apply(docId);
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

    private int oneByteToInt(int docId) {
        return (0xff & indexes.get(docId));
    }

    private int twoBytesToInt(int docId) {
        int byteIndex = docId * Short.BYTES;
        return (0xff & indexes.get(byteIndex)) << 8 |
                (0xff & indexes.get(byteIndex + 1));
    }

    private int fourBytesToInt(int docId) {
        return indexes.getInt(docId * Integer.BYTES);
    }
}
