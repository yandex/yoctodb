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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

final public class FoldedByteArrayIndexedList implements ByteArrayIndexedList {
    @NotNull
    private final Set<UnsignedByteArray> elements;
    @NotNull
    private final List<Long> offsets;
    @NotNull
    private final SortedMap<Integer, Integer> docIdOffsetIndex;
    private final int sizeOfIndexOffsetValue; // how many bites
    private final int databaseDocumentsCount;


    public FoldedByteArrayIndexedList(
            @NotNull final Map<UnsignedByteArray, LinkedList<Integer>> elements,
            int databaseDocumentsCount) {

        this.databaseDocumentsCount = databaseDocumentsCount;
        this.docIdOffsetIndex = new TreeMap<>();
        this.offsets = new ArrayList<>();
        this.elements = elements.keySet();

        long elementOffset = 0;
        // reserve first value for empty documents
        offsets.add(-1L);

        for (Map.Entry<UnsignedByteArray, LinkedList<Integer>> elem :
                elements.entrySet()) {
            UnsignedByteArray value = elem.getKey();
            offsets.add(elementOffset);

            final long currentElementOffset = elementOffset;
            elem.getValue().forEach( docId ->
                    docIdOffsetIndex
                         .put(docId,
                         offsets.indexOf(currentElementOffset)));
            elementOffset += value.getSizeInBytes();
        }

        offsets.add(elementOffset);
        offsets.set(0, 0L);

        // analyze result of offsets
        final int offsetCount = offsets.size();
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
        for (UnsignedByteArray element : elements) {
            elementSize += element.length();
        }

        return Integer.BYTES + // Element count in bytes
                Integer.BYTES + // offsets count in bytes
                sizeOfIndexOffsetValue *
                        databaseDocumentsCount + // indexes offsets in bytes
                Long.BYTES * offsets.size() + // offsets array size in bytes
                elementSize; // Element array size in bytes
    }

    @Override
    public void writeTo(
            @NotNull final OutputStream os) throws IOException {
        // elements count
        os.write(Ints.toByteArray(databaseDocumentsCount));

        // write offsets count!
        // before writing indexes because
        // you should to know how to deserialize it
        os.write(Ints.toByteArray(offsets.size()));

        List<Integer> offsetIndexes = new ArrayList<>();
        int expectedDocument = 0;
        for (Integer docId : docIdOffsetIndex.keySet()) {
            while (expectedDocument < docId) {
                offsetIndexes.add(0);
                expectedDocument++;
            }
            offsetIndexes.add(docIdOffsetIndex.get(docId));
            expectedDocument++;
        }
        while (expectedDocument < databaseDocumentsCount) {
            offsetIndexes.add(0);
            expectedDocument++;
        }

        // indexes of offsets - in correct Order
        switch (sizeOfIndexOffsetValue) {
            case (Byte.BYTES): {
                // write every int to one byte
                // The collection's iterator returns the elements
                // in ascending order of the corresponding keys.
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
        for (OutputStreamWritable e : elements) {
            e.writeTo(os);
        }
    }

    @Override
    public String toString() {
        return "FoldedByteArrayIndexedList{" +
                "elementsCount=" + docIdOffsetIndex.size() +
                '}';
    }

    private byte[] oneByteFromInteger(Integer data) {
        return new byte[]{
                (byte) ((data) & 0xff)
        };
    }

    private byte[] twoBytesFromInteger(Integer data) {
        return new byte[]{
                (byte) ((data >> 8) & 0xff),
                (byte) ((data) & 0xff)
        };
    }

}
