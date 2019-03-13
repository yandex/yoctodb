/*
 * (C) YANDEX LLC, 2014-2019
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link ByteArrayIndexedList} with variable sized elements
 *
 * @author irenkamalova
 */
@NotThreadSafe
public final class FoldedByteArrayIndexedList
        implements ByteArrayIndexedList {
    @NotNull
    private final Set<UnsignedByteArray> elements;
    @NotNull
    private final List<Long> offsets;
    @NotNull
    private final int[] docIdOffsetIndex;
    private final int sizeOfIndexOffsetValue;
    private final int databaseDocumentsCount;

    public FoldedByteArrayIndexedList(
            @NotNull final Map<UnsignedByteArray, List<Integer>> elements,
            final int databaseDocumentsCount) {
        this.databaseDocumentsCount = databaseDocumentsCount;
        this.docIdOffsetIndex = new int[databaseDocumentsCount];
        this.offsets = new ArrayList<>();
        this.elements = elements.keySet();

        long elementOffset = 0;
        // reserve first value for empty documents
        offsets.add(-1L);
        int currentIndex = 1;
        for (Map.Entry<UnsignedByteArray, List<Integer>> elem :
                elements.entrySet()) {
            final UnsignedByteArray value = elem.getKey();
            offsets.add(elementOffset);

            final int currentElementOffsetIndex = currentIndex;
            elem.getValue().forEach(docId ->
                    docIdOffsetIndex[docId] = currentElementOffsetIndex);
            currentIndex++;
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

    private static byte[] oneByteFromInteger(Integer data) {
        assert data < (1 << Byte.SIZE);
        return new byte[]{data.byteValue()};
    }

    private static byte[] twoBytesFromInteger(Integer data) {
        assert data < (1 << Short.SIZE);
        return Shorts.toByteArray(data.shortValue());
    }

    @Override
    public String toString() {
        return "FoldedByteArrayIndexedList{" +
                "elementsCount=" + docIdOffsetIndex.length +
                '}';
    }

    @Override
    public long getSizeInBytes() {
        final long elementSize =
                elements.stream()
                        .mapToInt(UnsignedByteArray::length)
                        .sum();

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

        // write offsets count
        os.write(Ints.toByteArray(offsets.size()));

        switch (sizeOfIndexOffsetValue) {
            case (Byte.BYTES): {
                // write every int to one byte
                // The collection's iterator returns the elements
                // in ascending order of the corresponding keys.
                for (Integer offsetIndex : docIdOffsetIndex) {
                    os.write(oneByteFromInteger(offsetIndex));
                }
                break;
            }
            case (Short.BYTES): {
                // write every int to two bytes
                for (Integer offsetIndex : docIdOffsetIndex) {
                    os.write(twoBytesFromInteger(offsetIndex));
                }
                break;
            }
            case (Integer.BYTES): {
                // write every int to four bytes
                for (Integer offsetIndex : docIdOffsetIndex) {
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
}
