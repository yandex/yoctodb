/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.immutable.util;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.UnsignedByteArrays;
import ru.yandex.yoctodb.util.immutable.ByteArrayIndexedList;
import ru.yandex.yoctodb.util.immutable.impl.FixedLengthByteArrayIndexedList;
import ru.yandex.yoctodb.util.immutable.impl.VariableLengthByteArrayIndexedList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author svyatoslav Date: 16.11.13
 */
public class ByteArrayIndexedListTest {
    @Test
    public void buildingFromFixedLengthByteArrayIndexedListTest()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        elements.add(UnsignedByteArrays.raw(new byte[]{0, 1, 2, 3}));
        elements.add(UnsignedByteArrays.raw(new byte[]{4, 5, 6, 7}));
        elements.add(UnsignedByteArrays.raw(new byte[]{8, 9, 10, 11}));
        elements.add(UnsignedByteArrays.raw(new byte[]{12, 13, 14, 15}));
        elements.add(UnsignedByteArrays.raw(new byte[]{16, 17, 18, 19}));

        final ByteBuffer bb =
                prepareDataFromFixedLengthByteArrayIndexedList(elements);
        final ByteArrayIndexedList list =
                FixedLengthByteArrayIndexedList.from(bb);

        Assert.assertEquals(elements.size(), list.size());

        for (int i = 0; i < elements.size(); i++) {
            Assert.assertEquals(
                    elements.get(i).toByteBuffer(),
                    list.get(i));
        }
    }

    private ByteBuffer prepareDataFromFixedLengthByteArrayIndexedList(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final ru.yandex.yoctodb.util.mutable.impl.FixedLengthByteArrayIndexedList fixedLengthByteArrayIndexedList =
                new ru.yandex.yoctodb.util.mutable.impl.FixedLengthByteArrayIndexedList();
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
    public void buildingFromVariableLengthByteArrayIndexedListTest()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        elements.add(UnsignedByteArrays.raw(new byte[]{0}));
        elements.add(UnsignedByteArrays.raw(new byte[]{1, 2}));
        elements.add(UnsignedByteArrays.raw(new byte[]{3, 4, 5}));
        elements.add(UnsignedByteArrays.raw(new byte[]{6, 7, 8, 9}));
        elements.add(UnsignedByteArrays.raw(new byte[]{10, 11, 12, 13, 14}));

        final ByteBuffer bb =
                prepareDataFromVariableLengthByteArrayIndexedLength(elements);
        final ByteArrayIndexedList list =
                VariableLengthByteArrayIndexedList.from(bb);

        for (int i = 0; i < elements.size(); i++) {
            Assert.assertEquals(
                    elements.get(i).toByteBuffer(),
                    list.get(i));
        }
    }

    private ByteBuffer prepareDataFromVariableLengthByteArrayIndexedLength(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final ru.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList variableLengthByteArrayIndexedList =
                new ru.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList();
        for (UnsignedByteArray element : elements) {
            variableLengthByteArrayIndexedList.add(element);
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        variableLengthByteArrayIndexedList.writeTo(os);
        Assert.assertEquals(
                os.size(),
                variableLengthByteArrayIndexedList.getSizeInBytes());

        return ByteBuffer.wrap(os.toByteArray());
    }
}
