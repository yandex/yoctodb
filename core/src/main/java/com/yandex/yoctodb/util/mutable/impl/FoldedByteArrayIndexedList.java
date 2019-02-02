package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final public class FoldedByteArrayIndexedList implements ByteArrayIndexedList {
    @NotNull
    private final List<UnsignedByteArray> elements;
    @NotNull
    private final List<UnsignedByteArray> uniqueElements;
    @NotNull
    private final List<Long> offsets;
    @NotNull
    private final List<Integer> offsetIndexes;
    @NotNull
    private final Map<UnsignedByteArray, Long> valueOffset;
    private final int sizeOfIndexOffsetValue; // how many bites


    public FoldedByteArrayIndexedList(
            @NotNull final List<UnsignedByteArray> elements) {
        this.elements = elements;
        this.offsetIndexes = new ArrayList<>();
        this.valueOffset = new HashMap<>();
        this.offsets = new ArrayList<>();
        this.uniqueElements = new ArrayList<>();

        long elementOffset = 0;
        for (int docId = 0; docId < elements.size(); docId++) {
            UnsignedByteArray elem = elements.get(docId);
            if (!valueOffset.containsKey(elements.get(docId))) {
                valueOffset.putIfAbsent(elements.get(docId), elementOffset);
                offsets.add(elementOffset);
                elementOffset += elem.getSizeInBytes();
                uniqueElements.add(elem);
            }
            offsetIndexes.add(offsets.indexOf(valueOffset.get(elem)));
        }

        offsets.add(elementOffset);

        // analyze result of offsets
        int offsetCount = offsets.size();
        if (offsetCount < (1 << Byte.SIZE)) {
            sizeOfIndexOffsetValue = Byte.BYTES;
        } else if (offsetCount < (1 << Short.SIZE)) {
            sizeOfIndexOffsetValue = Short.BYTES;
        } else {
            sizeOfIndexOffsetValue = Integer.BYTES;
        }

    }

    @Override
    public long getSizeInBytes() {
        long elementSize = 0;
        for (UnsignedByteArray element : uniqueElements) {
            elementSize += element.length();
        }

        return Integer.BYTES + // Element count in bytes
                Integer.BYTES + // offsets count in bytes
                sizeOfIndexOffsetValue *
                        elements.size() + // indexes offsets in bytes
                Long.BYTES * offsets.size() + // offsets array size in bytes
                elementSize; // Element array size in bytes
    }

    @Override
    public void writeTo(
            @NotNull final OutputStream os) throws IOException {
        // elements count
        os.write(Ints.toByteArray(elements.size()));

        // write offsets count!
        // before writing indexes because
        // you should to know how to deserialize it
        os.write(Ints.toByteArray(offsets.size()));

        // indexes of offsets - in correct Order
        switch (sizeOfIndexOffsetValue) {
            case (Byte.BYTES): {
                // write every int to one byte
                for (Integer offsetIndex : offsetIndexes) {
                    os.write(oneByteFromInteger(offsetIndex));
                }
                break;
            }
            case (Short.BYTES): {
                // write every int to two bytes
                for (Integer offsetIndex : offsetIndexes) {
                    os.write(twoBytesFromInteger(offsetIndex));
                }
                break;
            }
            case (Integer.BYTES): {
                // write every int to four bytes
                for (Integer offsetIndex : offsetIndexes) {
                    os.write(Ints.toByteArray(offsetIndex));
                }
                break;
            }
        }

        // offsets
        for (Long offset : offsets) {
            os.write(Longs.toByteArray(offset));
        }

        // elements
        for (OutputStreamWritable e : uniqueElements) {
            e.writeTo(os);
        }
    }

    @Override
    public String toString() {
        return "FoldedByteArrayIndexedList{" +
                "elementsCount=" + elements.size() +
                '}';
    }

    private byte[] oneByteFromInteger(Integer data) { // to  2^16 - 1 = 65535
        return new byte[] {
                (byte) ((data) & 0xff)
        };
    }

    private byte[] twoBytesFromInteger(Integer data) { // to  2^16 - 1 = 65535
        return new byte[] {
                (byte) ((data >> 8) & 0xff),
                (byte) ((data) & 0xff)
        };
    }

}
