package com.yandex.yoctodb.util.mutable.impl;

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
    private final List<UnsignedByteArray> uniqueElements;
    private final List<Long> offsets;
    private final List<Integer> offsetIndexes;
    private final Map<UnsignedByteArray, Long> valueOffset;
    private final long lastOffsetValue;


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

        this.lastOffsetValue = elementOffset;
    }

    @Override
    public long getSizeInBytes() {
//        long sizeOfIndexOffsetValue;
        int elementsCount = elements.size();
        // I will optimize it later
//        if (elementsSize <= 127) {
//            sizeOfIndexOffsetValue = 1L;
//        } else if (elementsSize <= 256) {
//            sizeOfIndexOffsetValue = 2L;
//        } else if (elementsSize <= 512) {
//            sizeOfIndexOffsetValue = 3L;
//        } else sizeOfIndexOffsetValue = 4L;

        long elementSize = 0;
        for (UnsignedByteArray element : uniqueElements) {
            elementSize += element.length();
        }

        return 4L + // Element count in bytes
                4L * (elements.size()) + // indexes offsets in bytes
                4L + // offsets count in bytes
                8L * (offsets.size() + 1) + // offsets array size in bytes
                elementSize; // Element array size in bytes
    }

    @Override
    public void writeTo(
            @NotNull final OutputStream os) throws IOException {
        // elements count
        os.write(Ints.toByteArray(elements.size()));

        // indexes of offsets - in correct Order
        for (Integer offsetIndex : offsetIndexes) {
            os.write(Ints.toByteArray(offsetIndex));
        }

        // write offsets count!
        // + 1 because of lastOffsetValue
        os.write(Ints.toByteArray(offsets.size() + 1));

        // offsets
        for (Long offset : offsets) {
            os.write(Longs.toByteArray(offset));
        }
        os.write(Longs.toByteArray(lastOffsetValue));

        // elements
        for (OutputStreamWritable e : uniqueElements)
            e.writeTo(os);
    }

    @Override
    public String toString() {
        return "FoldedByteArrayIndexedList{" +
                "elementsCount=" + elements.size() +
                '}';
    }

    public String state() {
        return "FoldedByteArrayIndexedList{" +
                "elementsCount=" + elements.size() + "\n" +
                elements.toString() + "\n" +
                uniqueElements.toString() + "\n" +
                offsetIndexes.toString() + "\n" +
                valueOffset.toString() + "\n" +
                offsets.toString() + "\n" +
                lastOffsetValue +
                '}';
    }
}
