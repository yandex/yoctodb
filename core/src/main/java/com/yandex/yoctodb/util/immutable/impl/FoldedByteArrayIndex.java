package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import org.jetbrains.annotations.NotNull;

public class FoldedByteArrayIndex implements ByteArrayIndexedList {
    private final int elementCount;
    private final int sizeOfIndexOffsetValue;
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
        if (offsetsCount <= 127) { // one byte 2^8 - 1 = 127
            sizeOfIndexOffsetValue = 1;
        } else if (offsetsCount <= 65535) {  // to  2^16 - 1 = 65535
            sizeOfIndexOffsetValue = 2;
        } else if (offsetsCount <= 16777215) {  // to 2^24 - 1 = 16777215
            sizeOfIndexOffsetValue = 3;
        } else {
            sizeOfIndexOffsetValue = 4;
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
        this.sizeOfIndexOffsetValue = sizeOfIndexOffsetValue;
        this.elements = elements;
        this.offsets = offsets;
        this.indexes = indexes;
    }

    @NotNull
    @Override
    public Buffer get(final int docId) {
        assert 0 <= docId && docId < elementCount;

        int offsetIndex = getOffsetIndex(docId);
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

    private int getOffsetIndex(int docId) {
        // если здесь не использовать slice - не ломается :)
        switch (sizeOfIndexOffsetValue) {
            case (1): {
                // write every int to one byte
                return indexes.get(docId);
            }
            case (2): {
                // write every int to two bytes
                return twoBytesToInt(docId);
            }
            case (3): {
                // write every int to three bytes
                return threeBytesToInt(docId);
            }
            case (4): {
                // write every int to four bytes
                return indexes.getInt(docId >> 2); // как и раньше
            }
        }
        throw new IllegalArgumentException();
    }

    private int twoBytesToInt(int docId) {
        int byteIndex = docId * 2;
        byte[] bytes = new byte[] {
                indexes.get(byteIndex),
                indexes.get(byteIndex + 1)
        };
        return (0xff & bytes[0]) << 8 | (0xff & bytes[1]);
    }

    private int threeBytesToInt(int docId) {
        int byteIndex = docId * 3;
        byte[] bytes = new byte[] {
                indexes.get(byteIndex),
                indexes.get(byteIndex + 1),
                indexes.get(byteIndex + 2)
        };
        return (0xff & bytes[0]) << 16 |
                (0xff & bytes[1]) << 8 |
                (0xff & bytes[2]);
    }
}
