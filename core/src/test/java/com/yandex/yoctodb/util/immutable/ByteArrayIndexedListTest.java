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
        final List<UnsignedByteArray> elements = new ArrayList<>();
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

    @Test
    public void buildingFromFixedLengthByteArrayIndexedListTestLongUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(0L));
        elements.add(UnsignedByteArrays.from(1L));
        elements.add(UnsignedByteArrays.from(2L));
        elements.add(UnsignedByteArrays.from(3L));
        elements.add(UnsignedByteArrays.from(4L));

        final Buffer bb =
                prepareDataFromFixedLengthByteArrayIndexedList(elements);
        final ByteArrayIndexedList list =
                FixedLengthByteArrayIndexedList.from(bb);

        Assert.assertEquals(elements.size(), list.size());

        for (int i = 0; i < elements.size(); ++i) {
            final long puttedValue = elements.get(i).toByteBuffer().getLong() ^ Long.MIN_VALUE;
            Assert.assertEquals(puttedValue, list.getLongUnsafe(i));
        }
    }

    @Test
    public void buildingFromFixedLengthByteArrayIndexedListTestIntUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(0));
        elements.add(UnsignedByteArrays.from(1));
        elements.add(UnsignedByteArrays.from(2));
        elements.add(UnsignedByteArrays.from(3));
        elements.add(UnsignedByteArrays.from(4));

        final Buffer bb =
                prepareDataFromFixedLengthByteArrayIndexedList(elements);
        final ByteArrayIndexedList list =
                FixedLengthByteArrayIndexedList.from(bb);

        Assert.assertEquals(elements.size(), list.size());

        for (int i = 0; i < elements.size(); ++i) {
            final long puttedValue = elements.get(i).toByteBuffer().getInt() ^ Integer.MIN_VALUE;
            Assert.assertEquals(puttedValue, list.getIntUnsafe(i));
        }
    }

    @Test
    public void buildingFromFixedLengthByteArrayIndexedListTestShortUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from((short) 0));
        elements.add(UnsignedByteArrays.from((short) 1));
        elements.add(UnsignedByteArrays.from((short) 2));
        elements.add(UnsignedByteArrays.from((short) 3));
        elements.add(UnsignedByteArrays.from((short) 4));

        final Buffer bb =
                prepareDataFromFixedLengthByteArrayIndexedList(elements);
        final ByteArrayIndexedList list =
                FixedLengthByteArrayIndexedList.from(bb);

        Assert.assertEquals(elements.size(), list.size());

        for (int i = 0; i < elements.size(); ++i) {
            final long puttedValue = elements.get(i).toByteBuffer().getShort() ^ Short.MIN_VALUE;
            Assert.assertEquals(puttedValue, list.getShortUnsafe(i));
        }
    }

    @Test
    public void buildingFromFixedLengthByteArrayIndexedListTestCharUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from('a'));
        elements.add(UnsignedByteArrays.from('b'));
        elements.add(UnsignedByteArrays.from('c'));
        elements.add(UnsignedByteArrays.from('d'));
        elements.add(UnsignedByteArrays.from('e'));

        final Buffer bb =
                prepareDataFromFixedLengthByteArrayIndexedList(elements);
        final ByteArrayIndexedList list =
                FixedLengthByteArrayIndexedList.from(bb);

        Assert.assertEquals(elements.size(), list.size());

        for (int i = 0; i < elements.size(); ++i) {
            final long puttedValue = elements.get(i).toByteBuffer().getChar();
            Assert.assertEquals(puttedValue, list.getCharUnsafe(i));
        }
    }

    @Test
    public void buildingFromFixedLengthByteArrayIndexedListTestByteUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from((byte) 0));
        elements.add(UnsignedByteArrays.from((byte) 1));
        elements.add(UnsignedByteArrays.from((byte) 2));
        elements.add(UnsignedByteArrays.from((byte) 3));
        elements.add(UnsignedByteArrays.from((byte) 4));

        final Buffer bb =
                prepareDataFromFixedLengthByteArrayIndexedList(elements);
        final ByteArrayIndexedList list =
                FixedLengthByteArrayIndexedList.from(bb);

        Assert.assertEquals(elements.size(), list.size());

        for (int i = 0; i < elements.size(); ++i) {
            final long puttedValue = elements.get(i).toByteBuffer().get() ^ Byte.MIN_VALUE;
            Assert.assertEquals(puttedValue, list.getByteUnsafe(i));
        }
    }

    private Buffer prepareDataFromFixedLengthByteArrayIndexedList(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArrayIndexedList fixedLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.FixedLengthByteArrayIndexedList(elements);

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
        final List<UnsignedByteArray> elements = new ArrayList<>();
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

    @Test
    public void buildingFromVariableLengthByteArrayIndexedListTestLongUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(0L));
        elements.add(UnsignedByteArrays.from(1L));
        elements.add(UnsignedByteArrays.from(2L));
        elements.add(UnsignedByteArrays.from(3L));
        elements.add(UnsignedByteArrays.from(4L));

        final Buffer bb =
                prepareDataFromVariableLengthByteArrayIndexedLength(elements);
        final ByteArrayIndexedList list =
                VariableLengthByteArrayIndexedList.from(bb);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getLong() ^ Long.MIN_VALUE;
            Assert.assertEquals(puttedValue, list.getLongUnsafe(i));
        }
    }

    @Test
    public void buildingFromVariableLengthByteArrayIndexedListTestIntUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from(0));
        elements.add(UnsignedByteArrays.from(1));
        elements.add(UnsignedByteArrays.from(2));
        elements.add(UnsignedByteArrays.from(3));
        elements.add(UnsignedByteArrays.from(4));

        final Buffer bb =
                prepareDataFromVariableLengthByteArrayIndexedLength(elements);
        final ByteArrayIndexedList list =
                VariableLengthByteArrayIndexedList.from(bb);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getInt() ^ Integer.MIN_VALUE;
            Assert.assertEquals(puttedValue, list.getIntUnsafe(i));
        }
    }

    @Test
    public void buildingFromVariableLengthByteArrayIndexedListTestShortUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from((short) 0));
        elements.add(UnsignedByteArrays.from((short) 1));
        elements.add(UnsignedByteArrays.from((short) 2));
        elements.add(UnsignedByteArrays.from((short) 3));
        elements.add(UnsignedByteArrays.from((short) 4));

        final Buffer bb =
                prepareDataFromVariableLengthByteArrayIndexedLength(elements);
        final ByteArrayIndexedList list =
                VariableLengthByteArrayIndexedList.from(bb);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getShort() ^ Short.MIN_VALUE;
            Assert.assertEquals(puttedValue, list.getShortUnsafe(i));
        }
    }

    @Test
    public void buildingFromVariableLengthByteArrayIndexedListTestCharUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from('a'));
        elements.add(UnsignedByteArrays.from('b'));
        elements.add(UnsignedByteArrays.from('c'));
        elements.add(UnsignedByteArrays.from('d'));
        elements.add(UnsignedByteArrays.from('e'));

        final Buffer bb =
                prepareDataFromVariableLengthByteArrayIndexedLength(elements);
        final ByteArrayIndexedList list =
                VariableLengthByteArrayIndexedList.from(bb);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().getChar();
            Assert.assertEquals(puttedValue, list.getCharUnsafe(i));
        }
    }

    @Test
    public void buildingFromVariableLengthByteArrayIndexedListTestByteUnsafe()
            throws IOException {
        //elements
        final List<UnsignedByteArray> elements = new ArrayList<>();
        elements.add(UnsignedByteArrays.from((byte) 0));
        elements.add(UnsignedByteArrays.from((byte) -2));
        elements.add(UnsignedByteArrays.from((byte) 34));
        elements.add(UnsignedByteArrays.from((byte) 21));
        elements.add(UnsignedByteArrays.from((byte) 13));

        final Buffer bb =
                prepareDataFromVariableLengthByteArrayIndexedLength(elements);
        final ByteArrayIndexedList list =
                VariableLengthByteArrayIndexedList.from(bb);

        for (int i = 0; i < elements.size(); i++) {
            final long puttedValue = elements.get(i).toByteBuffer().get() ^ Byte.MIN_VALUE;
            Assert.assertEquals(puttedValue, list.getByteUnsafe(i));
        }
    }

    private Buffer prepareDataFromVariableLengthByteArrayIndexedLength(
            final Collection<UnsignedByteArray> elements) throws IOException {
        final com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList variableLengthByteArrayIndexedList =
                new com.yandex.yoctodb.util.mutable.impl.VariableLengthByteArrayIndexedList(elements);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        variableLengthByteArrayIndexedList.writeTo(os);
        Assert.assertEquals(
                os.size(),
                variableLengthByteArrayIndexedList.getSizeInBytes());

        return Buffer.from(os.toByteArray());
    }
}
