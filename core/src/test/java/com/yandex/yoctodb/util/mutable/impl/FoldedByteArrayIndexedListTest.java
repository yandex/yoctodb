package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * {@link com.yandex.yoctodb.util.immutable.ByteArrayIndexedList} with fixed size
 * elements
 *
 * @author irenkamalova
 */
public class FoldedByteArrayIndexedListTest {

    @Test
    public void checkOutputStream() throws IOException {
        final List<UnsignedByteArray> strings = initString();
        Map<UnsignedByteArray, List<Integer>> data = initData(strings);
        final FoldedByteArrayIndexedList set =
                new FoldedByteArrayIndexedList(data, strings.size());
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        set.writeTo(os);

        Buffer buf = Buffer.from(os.toByteArray());
        // Full buffer size in bytes: 51
        assertEquals(51, buf.remaining());

        final int elementsCount = buf.getInt();
        assertEquals(elementsCount, strings.size());

        final int offsetsCount = buf.getInt();

        // Offsets count is: 4
        assertEquals(4, offsetsCount);

        int sizeOfIndexOffsetValue;
        if (offsetsCount <= 127) { // one byte 2^8 - 1 = 127
            sizeOfIndexOffsetValue = 1;
        } else if (offsetsCount <= 65535) {  // to  2^16 - 1 = 65535
            sizeOfIndexOffsetValue = 2;
        } else {
            sizeOfIndexOffsetValue = 4;
        }

        assertEquals(43, buf.remaining());

        // indexes of offsets
        final Buffer indexes = buf.slice((elementsCount) * sizeOfIndexOffsetValue);

        assertEquals(4, indexes.remaining());

        assertEquals(1, getOffsetIndex(indexes, 0, sizeOfIndexOffsetValue)); // todo в assert!
        assertEquals(2, getOffsetIndex(indexes, 1, sizeOfIndexOffsetValue));
        assertEquals(1, getOffsetIndex(indexes, 2, sizeOfIndexOffsetValue));

        long shift = indexes.remaining();

        // After slicing indexes shift is: 4
        assertEquals(4, shift);
        // After slicing indexes offsets size is: 39
        assertEquals(39, (buf.remaining() - shift));

        final Buffer offsets = buf.slice()
                .slice(shift, (offsetsCount) << 3);

        // Offset remaining = 32
        assertEquals(32, offsets.remaining());

        shift = shift + offsets.remaining();

        // After slicing indexes shift is: 36
        assertEquals(36, shift);
        // After slicing indexes offsets size is: 7
        assertEquals(7, (buf.remaining() - shift));

        final Buffer elements = buf.slice()
                .slice(shift, (buf.remaining() - shift));

        assertEquals(7, elements.remaining());

        // value 0 0
        assertEquals(0, getValueIndex(indexes, offsets, 0, sizeOfIndexOffsetValue));
        // value 1 3
        assertEquals(3, getValueIndex(indexes, offsets, 1, sizeOfIndexOffsetValue));
        // value 2 0
        assertEquals(0, getValueIndex(indexes, offsets, 2, sizeOfIndexOffsetValue));

        Buffer buffer;

        buffer = getValue(indexes, offsets, elements, 0, sizeOfIndexOffsetValue);
        UnsignedByteArray byteArray = UnsignedByteArrays.from(buffer);
        String value = UnsignedByteArrays.toString(byteArray);
        assertEquals("NEW", value);

        buffer = getValue(indexes, offsets, elements, 1, sizeOfIndexOffsetValue);
        byteArray = UnsignedByteArrays.from(buffer);
        value = UnsignedByteArrays.toString(byteArray);
        assertEquals("USED", value);

        buffer = getValue(indexes, offsets, elements, 2, sizeOfIndexOffsetValue);
        byteArray = UnsignedByteArrays.from(buffer);
        value = UnsignedByteArrays.toString(byteArray);
        assertEquals("NEW", value);

        assertEquals(getValueFromBuffer(getValue(indexes, offsets, elements, 0, sizeOfIndexOffsetValue)), "NEW");
        assertEquals(getValueFromBuffer(getValue(indexes, offsets, elements, 1, sizeOfIndexOffsetValue)), "USED");
        assertEquals(getValueFromBuffer(getValue(indexes, offsets, elements, 2, sizeOfIndexOffsetValue)), "NEW");
        assertEquals(getValueFromBuffer(getValue(indexes, offsets, elements, 3, sizeOfIndexOffsetValue)), "NEW");
    }

    private String getValueFromBuffer(Buffer buffer) {
        UnsignedByteArray byteArray = UnsignedByteArrays.from(buffer);
        return UnsignedByteArrays.toString(byteArray);
    }

    private Buffer getValue(Buffer indexes,
                            Buffer offsets,
                            Buffer elements,
                            int docId,
                            int sizeOfIndexOffsetValue) {
        int offsetIndex = getOffsetIndex(indexes, docId, sizeOfIndexOffsetValue);
        long start = offsets.getLong(offsetIndex << 3);
        long end = offsets.getLong((offsetIndex + 1) << 3); // берем соседа
        return elements.slice(start, end - start);
    }


    private long getValueIndex(Buffer indexes,
                               Buffer offsets,
                               int docId,
                               int sizeOfIndexOffsetValue) {
        int offsetIndex = getOffsetIndex(indexes, docId, sizeOfIndexOffsetValue);
        return offsets.getLong(offsetIndex << 3);
    }

    private int getOffsetIndex(Buffer indexes, int docId, int sizeOfIndexOffsetValue) {
        switch (sizeOfIndexOffsetValue) {
            case (Byte.BYTES): {
                // write every int to one byte
                return indexes.get(docId);
            }
            case (Short.BYTES): {
                // write every int to two bytes
                return twoBytesToInt(indexes, docId);
            }
            case (Integer.BYTES): {
                // write every int to four bytes
                return indexes.getInt(docId >> 2); // как и раньше
            }
        }
        throw new IllegalArgumentException();
    }

    private int twoBytesToInt(Buffer indexes, int docId) {
        int byteIndex = docId * Short.BYTES;
        byte[] bytes = new byte[]{
                indexes.get(byteIndex),
                indexes.get(byteIndex + 1)
        };
        return (0xff & bytes[0]) << 8 | (0xff & bytes[1]);
    }

    @Test
    public void checkSize() {
        final FoldedByteArrayIndexedList foldedList =
                new FoldedByteArrayIndexedList(initData(initString()), initString().size());
        assertEquals(51, foldedList.getSizeInBytes());
    }

    @Test
    public void string() {
        final List<UnsignedByteArray> elements = new LinkedList<>();
        final int size = 10;
        for (int i = 0; i < size; i++)
            elements.add(from(i));
        final ByteArrayIndexedList set =
                new FoldedByteArrayIndexedList(initData(elements), elements.size());
        final String text = set.toString();
        assertTrue(text.contains(Integer.toString(size)));
    }

    private final List<UnsignedByteArray> initString() {
        final List<UnsignedByteArray> elements = new LinkedList<>();
        elements.add(from("NEW"));
        elements.add(from("USED"));
        elements.add(from("NEW"));
        elements.add(from("NEW"));
        return elements;
    }

    private final Map<UnsignedByteArray, List<Integer>> initData(List<UnsignedByteArray> elements) {
        Map<UnsignedByteArray, List<Integer>> values = new LinkedHashMap<>();
        for (int i = 0; i < elements.size(); i++) {
            int documentId = i;
            UnsignedByteArray val = elements.get(documentId);
            values.merge(val,
                    new LinkedList<Integer>() {{
                        add(documentId);
                    }},
                    (oldList, newList) -> {
                        oldList.addAll(newList);
                        return oldList;
                    });
        }
        return values;
    }
}
