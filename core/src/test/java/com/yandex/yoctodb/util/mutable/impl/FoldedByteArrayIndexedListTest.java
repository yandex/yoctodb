package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class FoldedByteArrayIndexedListTest {

    @Test
    public void checkOutputStream() throws IOException {
        List<UnsignedByteArray> list = initString();
        final FoldedByteArrayIndexedList set =
                new FoldedByteArrayIndexedList(list);
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        set.writeTo(os);

        Buffer buf = Buffer.from(os.toByteArray());
        System.out.println("Full buffer size in bytes: " + buf.remaining());

        // getInt() возвращает (первый Int?) количество элементов,
        // которое мы записали в буфер
        final int elementsCount = buf.getInt();

        assertEquals(elementsCount, list.size());

        System.out.println("Remaining: " + buf.remaining());
        System.out.println(buf.slice());
        // indexes of offsets
        final Buffer indexes = buf.slice((elementsCount) << 2);

        System.out.println("Idexes remaining " + indexes.remaining());

        long shift = indexes.remaining();

        final int offsetsCount = buf.slice()
                .position(shift)
                .slice().getInt();

        System.out.println(offsetsCount);

        shift = shift + 4;
        // then offsets of element value
        final Buffer offsets = buf.slice() // получаем здесь копию buf
                .position(shift) // смещаемся до места, с которого начинаются offsets
                .slice((offsetsCount) << 3); // отрезаем столько, сколько offsets занимают!

        System.out.println("Offset remaining " + offsets.remaining());

        shift = shift + offsets.remaining();

        // then elements value
        final Buffer elements = buf.slice()
                .position(shift)
                .slice();

        System.out.println("Elements remaining " + elements.remaining());

        System.out.println("remaining " + indexes.remaining());
        System.out.println("value 0 " + getOffsetIndex(indexes, 0)); // todo в assert!
        System.out.println("value 1 " + getOffsetIndex(indexes, 1));
        System.out.println("value 2 " + getOffsetIndex(indexes, 2));
        System.out.println("remaining " + indexes.remaining());

        System.out.println("remaining " + offsets.remaining());
        System.out.println("value 0 " + getValueIndex(indexes, offsets, 0));
        System.out.println("value 1 " + getValueIndex(indexes, offsets, 1));
        System.out.println("value 2 " + getValueIndex(indexes, offsets, 2));
        System.out.println("remaining " + offsets.remaining());

        // осталось 7 бит - это NEW + USED!
        Buffer buffer;

        buffer = getValue(indexes, offsets, elements, 0);
        UnsignedByteArray byteArray = UnsignedByteArrays.from(buffer);
        String value = UnsignedByteArrays.toString(byteArray);
        System.out.println(value);

        buffer = getValue(indexes, offsets, elements, 1);
        byteArray = UnsignedByteArrays.from(buffer);
        value = UnsignedByteArrays.toString(byteArray);
        System.out.println(value);

        buffer = getValue(indexes, offsets, elements, 2);
        byteArray = UnsignedByteArrays.from(buffer);
        value = UnsignedByteArrays.toString(byteArray);
        System.out.println(value);

        assertEquals(getValueFromBuffer(getValue(indexes, offsets, elements, 0)), "NEW");
        assertEquals(getValueFromBuffer(getValue(indexes, offsets, elements, 1)), "USED");
        assertEquals(getValueFromBuffer(getValue(indexes, offsets, elements, 2)), "NEW");
        assertEquals(getValueFromBuffer(getValue(indexes, offsets, elements, 3)), "NEW");

    }

    private String getValueFromBuffer(Buffer buffer) {
        UnsignedByteArray byteArray = UnsignedByteArrays.from(buffer);
        return UnsignedByteArrays.toString(byteArray);
    }

    private Buffer getValue(Buffer indexes, Buffer offsets, Buffer elements, int docId) {
        int offsetIndex = indexes.getInt(docId << 2);
        long start = offsets.getLong(offsetIndex << 3);
        long end = offsets.getLong((offsetIndex + 1) << 3); // берем соседа
        return elements.slice(start, end - start);
    }


    private long getValueIndex(Buffer indexes, Buffer offsets, int docId) {
        // если здесь не использовать slice - не ломается :)
        int offsetIndex = indexes.getInt(docId << 2);
        return offsets.getLong(offsetIndex << 3);
    }

    private int getOffsetIndex(Buffer indexes, int docId) {
        // если здесь не использовать slice - не ломается :)
        return indexes.getInt(docId << 2);
    }

    @Test
    public void checkSize() {
        final FoldedByteArrayIndexedList foldedList =
                new FoldedByteArrayIndexedList(initString());
        assertEquals(foldedList.getSizeInBytes(), 55);
    }


    @Test
    public void state() {
        final FoldedByteArrayIndexedList foldedList =
                new FoldedByteArrayIndexedList(initString());
        final String text = foldedList.state();
        System.out.println(text);
        assertTrue(text.contains("[0, 1, 0, 0]"));
        assertTrue(text.contains("[0, 3]"));
        assertTrue(text.contains("7"));
    }

    @Test
    public void string() {
        final List<UnsignedByteArray> elements = new LinkedList<>();
        final int size = 10;
        for (int i = 0; i < size; i++)
            elements.add(from(i));
        final ByteArrayIndexedList set =
                new FoldedByteArrayIndexedList(elements);
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

}