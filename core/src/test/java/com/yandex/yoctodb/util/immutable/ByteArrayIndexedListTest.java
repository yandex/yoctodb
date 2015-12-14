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

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.immutable.impl.FixedLengthByteArrayIndexedList;
import com.yandex.yoctodb.util.immutable.impl.VariableLengthByteArrayIndexedList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author svyatoslav
 */
public class ByteArrayIndexedListTest {
    @Test
    public void buildingFromFixedLengthByteArrayIndexedListTest()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        elements.add(UnsignedByteArrays.from(new byte[]{0, 1, 2, 3}));
        elements.add(UnsignedByteArrays.from(new byte[]{4, 5, 6, 7}));
        elements.add(UnsignedByteArrays.from(new byte[]{8, 9, 10, 11}));
        elements.add(UnsignedByteArrays.from(new byte[]{12, 13, 14, 15}));
        elements.add(UnsignedByteArrays.from(new byte[]{16, 17, 18, 19}));

        final Buffer bb =
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

    private Buffer prepareDataFromFixedLengthByteArrayIndexedList(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArrayIndexedList fixedLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArrayIndexedList();
        for (UnsignedByteArray element : elements) {
            fixedLengthByteArrayIndexedList.add(element);
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        fixedLengthByteArrayIndexedList.writeTo(os);
        Assert.assertEquals(
                os.size(),
                fixedLengthByteArrayIndexedList.getSizeInBytes());

        return Buffer.from(os.toByteArray());
    }

    @Test
    public void buildingFromVariableLengthByteArrayIndexedListTest()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        elements.add(UnsignedByteArrays.from(new byte[]{0}));
        elements.add(UnsignedByteArrays.from(new byte[]{1, 2}));
        elements.add(UnsignedByteArrays.from(new byte[]{3, 4, 5}));
        elements.add(UnsignedByteArrays.from(new byte[]{6, 7, 8, 9}));
        elements.add(UnsignedByteArrays.from(new byte[]{10, 11, 12, 13, 14}));

        final Buffer bb =
                prepareDataFromVariableLengthByteArrayIndexedLength(elements);
        final ByteArrayIndexedList list =
                VariableLengthByteArrayIndexedList.from(bb);

        for (int i = 0; i < elements.size(); i++) {
            Assert.assertEquals(
                    elements.get(i).toByteBuffer(),
                    list.get(i));
        }
    }

    private Buffer prepareDataFromVariableLengthByteArrayIndexedLength(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList variableLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList();
        for (UnsignedByteArray element : elements) {
            variableLengthByteArrayIndexedList.add(element);
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        variableLengthByteArrayIndexedList.writeTo(os);
        Assert.assertEquals(
                os.size(),
                variableLengthByteArrayIndexedList.getSizeInBytes());

        return Buffer.from(os.toByteArray());
    }
}
