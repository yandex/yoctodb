/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        elements.add(UnsignedByteArrays.raw(new byte[]{8, 9, 10, 11}));
        elements.add(UnsignedByteArrays.raw(new byte[]{16, 17, 18, 19}));
        elements.add(UnsignedByteArrays.raw(new byte[]{12, 13, 14, 15}));
        elements.add(UnsignedByteArrays.raw(new byte[]{4, 5, 6, 7}));
        elements.add(UnsignedByteArrays.raw(new byte[]{0, 1, 2, 3}));

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

    private Buffer prepareDataFromFixedLengthByteArraySortedSet(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet fixedLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet();
        for (UnsignedByteArray element : elements) {
            fixedLengthByteArrayIndexedList.add(element);
        }

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
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        elements.add(UnsignedByteArrays.raw(new byte[]{1, 2}));
        elements.add(UnsignedByteArrays.raw(new byte[]{10, 11, 12, 13, 14}));
        elements.add(UnsignedByteArrays.raw(new byte[]{6, 7, 8, 9}));
        elements.add(UnsignedByteArrays.raw(new byte[]{3, 4, 5}));
        elements.add(UnsignedByteArrays.raw(new byte[]{0}));

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

    private Buffer prepareDataFromVariableLengthByteArraySortedSet(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet fixedLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet();
        for (UnsignedByteArray element : elements) {
            fixedLengthByteArrayIndexedList.add(element);
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        fixedLengthByteArrayIndexedList.writeTo(os);
        assertEquals(
                os.size(),
                fixedLengthByteArrayIndexedList.getSizeInBytes());

        return Buffer.from(os.toByteArray());
    }

    @Test
    public void lessThanTest() throws IOException {
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
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
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
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
