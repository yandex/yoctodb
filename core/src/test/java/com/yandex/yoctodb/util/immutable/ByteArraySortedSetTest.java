/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.impl.FixedLengthByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArraySortedSet;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;

/**
 * @author svyatoslav
 */
public class ByteArraySortedSetTest {
    private final int SIZE = 128;

    @Test
    public void buildingFromFixedLengthByteArraySortedSetTest()
            throws IOException {
        //unsorted elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(new byte[]{8, 9, 10, 11}));
        elements.add(UnsignedByteArrays.from(new byte[]{16, 17, 18, 19}));
        elements.add(UnsignedByteArrays.from(new byte[]{12, 13, 14, 15}));
        elements.add(UnsignedByteArrays.from(new byte[]{4, 5, 6, 7}));
        elements.add(UnsignedByteArrays.from(new byte[]{0, 1, 2, 3}));

        final Buffer bb =
                prepareDataFromFixedLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                FixedLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            assertEquals(
                    elements.get(i).toByteBuffer(),
                    ss.get(i));
        }
    }

    @Test
    public void buildingFromFixedLengthByteArraySortedSetTestLongUnsafe()
            throws IOException {
        //unsorted elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(0L));
        elements.add(UnsignedByteArrays.from(1L));
        elements.add(UnsignedByteArrays.from(2L));
        elements.add(UnsignedByteArrays.from(3L));
        elements.add(UnsignedByteArrays.from(4L));

        final Buffer bb =
                prepareDataFromFixedLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                FixedLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getLong() ^ Long.MIN_VALUE;
            assertEquals(puttedValue, ss.getLongUnsafe(i));
        }
    }

    @Test
    public void buildingFromFixedLengthByteArraySortedSetTestIntUnsafe()
            throws IOException {
        //unsorted elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(0));
        elements.add(UnsignedByteArrays.from(1));
        elements.add(UnsignedByteArrays.from(2));
        elements.add(UnsignedByteArrays.from(3));
        elements.add(UnsignedByteArrays.from(4));

        final Buffer bb =
                prepareDataFromFixedLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                FixedLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getInt() ^ Integer.MIN_VALUE;
            assertEquals(puttedValue, ss.getIntUnsafe(i));
        }
    }

    @Test
    public void buildingFromFixedLengthByteArraySortedSetTestShortUnsafe()
            throws IOException {
        //unsorted elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from((short) 0));
        elements.add(UnsignedByteArrays.from((short) 1));
        elements.add(UnsignedByteArrays.from((short) 2));
        elements.add(UnsignedByteArrays.from((short) 3));
        elements.add(UnsignedByteArrays.from((short) 4));

        final Buffer bb =
                prepareDataFromFixedLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                FixedLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getShort() ^ Short.MIN_VALUE;
            assertEquals(puttedValue, ss.getShortUnsafe(i));
        }
    }

    @Test
    public void buildingFromFixedLengthByteArraySortedSetTestCharUnsafe()
            throws IOException {
        //unsorted elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from('a'));
        elements.add(UnsignedByteArrays.from('b'));
        elements.add(UnsignedByteArrays.from('c'));
        elements.add(UnsignedByteArrays.from('d'));
        elements.add(UnsignedByteArrays.from('e'));

        final Buffer bb =
                prepareDataFromFixedLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                FixedLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getChar();
            assertEquals(puttedValue, ss.getCharUnsafe(i));
        }
    }

    @Test
    public void buildingFromFixedLengthByteArraySortedSetTestByteUnsafe()
            throws IOException {
        //unsorted elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from((byte) -128));
        elements.add(UnsignedByteArrays.from((byte) 123));
        elements.add(UnsignedByteArrays.from((byte) 2));
        elements.add(UnsignedByteArrays.from((byte) 3));
        elements.add(UnsignedByteArrays.from((byte) 127));

        final Buffer bb =
                prepareDataFromFixedLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                FixedLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().get() ^ Byte.MIN_VALUE;
            assertEquals(puttedValue, ss.getByteUnsafe(i));
        }
    }

    private Buffer prepareDataFromFixedLengthByteArraySortedSet(
            final Collection<UnsignedByteArray> items) throws IOException {
        final SortedSet<UnsignedByteArray> elements =
                new TreeSet<>();
        for (UnsignedByteArray element : items) {
            elements.add(element);
        }
        final com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet fixedLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet(elements);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        fixedLengthByteArrayIndexedList.writeTo(os);
        assertEquals(
                os.size(),
                fixedLengthByteArrayIndexedList.getSizeInBytes());

        return Buffer.from(os.toByteArray());
    }

    @Test
    public void buildingFromVariableLengthByteArraySortedSetTest()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(new byte[]{1, 2}));
        elements.add(UnsignedByteArrays.from(new byte[]{10, 11, 12, 13, 14}));
        elements.add(UnsignedByteArrays.from(new byte[]{6, 7, 8, 9}));
        elements.add(UnsignedByteArrays.from(new byte[]{3, 4, 5}));
        elements.add(UnsignedByteArrays.from(new byte[]{0}));

        final Buffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            assertEquals(
                    elements.get(i).toByteBuffer(),
                    ss.get(i));
        }
    }

    @Test
    public void buildingFromVariableLengthByteArraySortedSetTestLongUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(0L));
        elements.add(UnsignedByteArrays.from(1L));
        elements.add(UnsignedByteArrays.from(2L));
        elements.add(UnsignedByteArrays.from(3L));
        elements.add(UnsignedByteArrays.from(4L));

        final Buffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getLong() ^ Long.MIN_VALUE;
            assertEquals(puttedValue, ss.getLongUnsafe(i));
        }
    }

    @Test
    public void buildingFromVariableLengthByteArraySortedSetTestIntUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(0));
        elements.add(UnsignedByteArrays.from(1));
        elements.add(UnsignedByteArrays.from(2));
        elements.add(UnsignedByteArrays.from(3));
        elements.add(UnsignedByteArrays.from(4));

        final Buffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getInt() ^ Integer.MIN_VALUE;
            assertEquals(puttedValue, ss.getIntUnsafe(i));
        }
    }

    @Test
    public void buildingFromVariableLengthByteArraySortedSetTestShortUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from((short) 0));
        elements.add(UnsignedByteArrays.from((short) 1));
        elements.add(UnsignedByteArrays.from((short) 2));
        elements.add(UnsignedByteArrays.from((short) 3));
        elements.add(UnsignedByteArrays.from((short) 4));

        final Buffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getShort() ^ Short.MIN_VALUE;
            assertEquals(puttedValue, ss.getShortUnsafe(i));
        }
    }

    @Test
    public void buildingFromVariableLengthByteArraySortedSetTestCharUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from('a'));
        elements.add(UnsignedByteArrays.from('b'));
        elements.add(UnsignedByteArrays.from('c'));
        elements.add(UnsignedByteArrays.from('d'));
        elements.add(UnsignedByteArrays.from('e'));

        final Buffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getChar();
            assertEquals(puttedValue, ss.getCharUnsafe(i));
        }
    }

    @Test
    public void buildingFromVariableLengthByteArraySortedSetTestByteUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from((byte) 0));
        elements.add(UnsignedByteArrays.from((byte) 1));
        elements.add(UnsignedByteArrays.from((byte) 2));
        elements.add(UnsignedByteArrays.from((byte) 3));
        elements.add(UnsignedByteArrays.from((byte) 4));

        final Buffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);

        assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().get() ^ Byte.MIN_VALUE;
            assertEquals(puttedValue, ss.getByteUnsafe(i));
        }
    }

    private Buffer prepareDataFromVariableLengthByteArraySortedSet(
            final Collection<UnsignedByteArray> items) throws IOException {
        final SortedSet<UnsignedByteArray> elements =
                new TreeSet<>();
        for (UnsignedByteArray element : items) {
            elements.add(element);
        }
        final com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet fixedLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet(elements);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        fixedLengthByteArrayIndexedList.writeTo(os);
        assertEquals(
                os.size(),
                fixedLengthByteArrayIndexedList.getSizeInBytes());

        return Buffer.from(os.toByteArray());
    }

    @Test
    public void lessThanTest() throws IOException {
        final List<UnsignedByteArray> elements = new ArrayList<>();
        for (int i = 0; i < SIZE; i++) {
            elements.add(from(i));
        }
        final Buffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);
        for (int i = 0; i < SIZE; i++) {
            final Buffer buffer = from(i).toByteBuffer();

            assertEquals(i, ss.indexOf(buffer));

            assertEquals(
                    i,
                    ss.indexOfLessThan(buffer, true, 0));

            final int indexWithoutOrEquals =
                    ss.indexOfLessThan(buffer, false, 0);

            if (i == 0) {
                assertEquals(-1, indexWithoutOrEquals);
            } else {
                assertEquals(i - 1, indexWithoutOrEquals);
            }
        }

        assertEquals(
                -1,
                ss.indexOfLessThan(from(0).toByteBuffer(), false, 1));
    }

    @Test
    public void greaterThanTest() throws IOException {
        final List<UnsignedByteArray> elements = new ArrayList<>();
        for (int i = 0; i < SIZE; i++) {
            elements.add(from(i));
        }
        final Buffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);
        for (int i = 0; i < SIZE; i++) {
            final Buffer buffer = from(i).toByteBuffer();

            assertEquals(i, ss.indexOf(buffer));

            assertEquals(
                    i,
                    ss.indexOfGreaterThan(buffer, true, SIZE - 1));

            final int indexWithoutOrEquals =
                    ss.indexOfGreaterThan(buffer, false, SIZE - 1);

            if (i == SIZE - 1) {
                assertEquals(-1, indexWithoutOrEquals);
            } else {
                assertEquals(i + 1, indexWithoutOrEquals);
            }
        }

        assertEquals(
                -1,
                ss.indexOfGreaterThan(
                        from(SIZE - 1).toByteBuffer(),
                        false,
                        SIZE - 2));
    }
}
