/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.immutable.util;

import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.immutable.ByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.FixedLengthByteArraySortedSet;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArraySortedSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author svyatoslav Date: 17.11.13
 */
public class ByteArraySortedSetTest {
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

        final ByteBuffer bb =
                prepareDataFromFixedLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                FixedLengthByteArraySortedSet.from(bb);

        Assert.assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            Assert.assertEquals(
                    elements.get(i).toByteBuffer(),
                    ss.get(i));
        }
    }

    private ByteBuffer prepareDataFromFixedLengthByteArraySortedSet(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet fixedLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArraySortedSet();
        for (UnsignedByteArray element : elements) {
            fixedLengthByteArrayIndexedList.add(element);
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        fixedLengthByteArrayIndexedList.writeTo(os);
        Assert.assertEquals(
                os.size(),
                fixedLengthByteArrayIndexedList.getSizeInBytes());

        return ByteBuffer.wrap(os.toByteArray());
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

        final ByteBuffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);

        Assert.assertEquals(elements.size(), ss.size());

        //sorting to compare
        Collections.sort(elements);

        for (int i = 0; i < elements.size(); i++) {
            Assert.assertEquals(
                    elements.get(i).toByteBuffer(),
                    ss.get(i));
        }
    }

    private ByteBuffer prepareDataFromVariableLengthByteArraySortedSet(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet fixedLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArraySortedSet();
        for (UnsignedByteArray element : elements) {
            fixedLengthByteArrayIndexedList.add(element);
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        fixedLengthByteArrayIndexedList.writeTo(os);
        Assert.assertEquals(
                os.size(),
                fixedLengthByteArrayIndexedList.getSizeInBytes());

        return ByteBuffer.wrap(os.toByteArray());
    }

    @Test
    public void lessThanTest() throws IOException {
        final int size = 128;
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < size; i++) {
            elements.add(UnsignedByteArrays.from(i));
        }
        final ByteBuffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);
        for (int i = 0; i < size; i++) {
            final int index =
                    ss.indexOf(UnsignedByteArrays.from(i).toByteBuffer());
            Assert.assertEquals(i, index);

            final int indexWithOrEquals =
                    ss.indexOfLessThan(
                            UnsignedByteArrays.from(i).toByteBuffer(),
                            true,
                            0);
            Assert.assertEquals(i, indexWithOrEquals);

            final int indexWithoutOrEquals =
                    ss.indexOfLessThan(
                            UnsignedByteArrays.from(i).toByteBuffer(),
                            false,
                            0);
            Assert.assertEquals(i, indexWithoutOrEquals + 1);
        }
    }

    @Test
    public void greaterThanTest() throws IOException {
        final int size = 128;
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < size; i++) {
            elements.add(UnsignedByteArrays.from(i));
        }
        final ByteBuffer bb =
                prepareDataFromVariableLengthByteArraySortedSet(elements);
        final ByteArraySortedSet ss =
                VariableLengthByteArraySortedSet.from(bb);
        for (int i = 0; i < size; i++) {
            final int index =
                    ss.indexOf(UnsignedByteArrays.from(i).toByteBuffer());
            Assert.assertEquals(i, index);

            final int indexWithOrEquals =
                    ss.indexOfGreaterThan(
                            UnsignedByteArrays.from(i).toByteBuffer(),
                            true,
                            size - 1);
            Assert.assertEquals(i, indexWithOrEquals);

            final int indexWithoutOrEquals =
                    ss.indexOfGreaterThan(
                            UnsignedByteArrays.from(i).toByteBuffer(),
                            false,
                            size - 1);
            if (i == size - 1) {
                Assert.assertEquals(-1, indexWithoutOrEquals);
            } else {
                Assert.assertEquals(i, indexWithoutOrEquals - 1);
            }
        }
    }
}
