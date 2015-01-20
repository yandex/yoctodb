/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable.segment;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.yandex.yoctodb.v1.mutable.segment.Utils.calculateDigest;

/**
 * @author svyatoslav
 *         Date: 07.11.13
 */
public class V1FilterableIndexBinaryTest {

    @Test
    public void writingVariableLengthTest() throws IOException {
        final V1FilterableIndex v1FilterableIndex = new V1FilterableIndex("variable_length_field_name", false);

        //first doc
        final Collection<UnsignedByteArray> byteArraysDoc1 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{0}));
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{1, 2}));
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{3, 4, 5}));
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{6, 7, 8, 9}));
        byteArraysDoc1.add(
                UnsignedByteArrays.raw(
                        new byte[]{
                                10,
                                11,
                                12,
                                13,
                                14}
                )
        );
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{15}));
        v1FilterableIndex.addDocument(0, byteArraysDoc1);

        //second doc
        final Collection<UnsignedByteArray> byteArraysDoc2 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{0, 1, 2, 3, 4}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{5, 6, 7, 8}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{9, 10, 11}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{12, 13,}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{14}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{15}));
        v1FilterableIndex.addDocument(1, byteArraysDoc2);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        v1FilterableIndex.setDatabaseDocumentsCount(2);
        final OutputStreamWritable outputStreamWritable = v1FilterableIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 12);

        final Buffer byteBuffer = Buffer.from(os.toByteArray());
        final long fullSizeInBytes = byteBuffer.getLong();

        Assert.assertEquals(fullSizeInBytes, outputStreamWritable.getSizeInBytes());

        final int segmentTypeCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.VARIABLE_LENGTH_FILTER.getCode(), segmentTypeCode);

        final byte[] digest = calculateDigest(byteBuffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

        final int fieldNameLength = byteBuffer.getInt();
        Assert.assertEquals("variable_length_field_name".getBytes().length, fieldNameLength);

        final byte[] fieldNameBuffer = new byte[fieldNameLength];
        byteBuffer.get(fieldNameBuffer);
        final String fieldName = new String(fieldNameBuffer);
        Assert.assertEquals("variable_length_field_name", fieldName);

        //Reading values
        final long valuesSize = byteBuffer.getLong();
        Assert.assertEquals(87, valuesSize);

        final int maxElement = byteBuffer.getInt();
        Assert.assertEquals(5, maxElement);

        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(11, elementsCount);

        final int[] elementOffsets = new int[elementsCount + 1];

        for (int i = 0; i <= elementsCount; i++) {
            final int currentOffset = byteBuffer.getInt();
            elementOffsets[i] = currentOffset;
        }

        Assert.assertArrayEquals(new int[]{0, 1, 6, 8, 11, 15, 19, 22, 27, 29, 30, 31}, elementOffsets);

        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[elementOffsets[i + 1] - elementOffsets[i]];
            byteBuffer.get(currentElementBytes);
            elements.add(UnsignedByteArrays.raw(currentElementBytes));
        }
        //elements - should equals to sorted collection of bytes (byteArraysDoc1 merged with byteArraysDoc2)
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{0}), elements.get(0));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{0, 1, 2, 3, 4}), elements.get(1));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{1, 2}), elements.get(2));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{3, 4, 5}), elements.get(3));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{5, 6, 7, 8}), elements.get(4));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{6, 7, 8, 9}), elements.get(5));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{9, 10, 11}), elements.get(6));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{10, 11, 12, 13, 14}), elements.get(7));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{12, 13}), elements.get(8));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{14}), elements.get(9));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{15}), elements.get(10));
        //Reading indexes
        final long indexesSize = byteBuffer.getLong();
        Assert.assertEquals(100, indexesSize);
        final int code = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode(), code);


        final int keysCount = byteBuffer.getInt();
        Assert.assertEquals(11, keysCount);
//        final int[] indexOffsets = new int[keysCount];
//
//        for (int i = 0; i < keysCount; i++) {
//            final int currentOffset = byteBuffer.getInt();
//            indexOffsets[i] = currentOffset;
//        }
//        Assert.assertArrayEquals(new int[]{0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80}, indexOffsets);
//
//        final int[][] indexes = new int[11][];
//        for (int i = 0; i < keysCount; i++) {
//            int currentSetSize = byteBuffer.getInt();
//            indexes[i] = new int[currentSetSize];
//            for (int j = 0; j < currentSetSize; j++) {
//                int index = byteBuffer.getInt();
//                indexes[i][j] = index;
//            }
//        }
//
//        Assert.assertArrayEquals(new int[]{0}, indexes[0]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[1]);
//        Assert.assertArrayEquals(new int[]{0}, indexes[2]);
//        Assert.assertArrayEquals(new int[]{0}, indexes[3]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[4]);
//        Assert.assertArrayEquals(new int[]{0}, indexes[5]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[6]);
//        Assert.assertArrayEquals(new int[]{0}, indexes[7]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[8]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[9]);
//        Assert.assertArrayEquals(new int[]{0, 1}, indexes[10]);
//
//        int digestSize = byteBuffer.getInt();
//        Assert.assertEquals(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES, digestSize);
//        for (int i = 0; i < digestSize; i++) {
//            byte actualDigestByte = byteBuffer.get();
//            Assert.assertEquals(digest[i], actualDigestByte);
//        }
//
//        //check that input has not remaining
//        Assert.assertFalse(byteBuffer.hasRemaining());
    }

    @Test
    public void writingFixedLengthTest() throws IOException {
        final V1FilterableIndex v1FilterableIndex = new V1FilterableIndex("fixed_length_field_name", true);

        //first doc
        final Collection<UnsignedByteArray> byteArraysDoc1 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{0, 1, 2, 3}));
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{4, 5, 6, 7}));
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{8, 9, 10, 11}));
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{12, 13, 14, 15}));
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{16, 17, 18, 19}));
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{7, 7, 7, 7}));
        v1FilterableIndex.addDocument(0, byteArraysDoc1);

        //second doc
        final Collection<UnsignedByteArray> byteArraysDoc2 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{19, 18, 17, 16}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{15, 14, 13, 12}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{11, 10, 9, 8}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{7, 6, 5, 4}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}));
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{7, 7, 7, 7}));
        v1FilterableIndex.addDocument(1, byteArraysDoc2);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        v1FilterableIndex.setDatabaseDocumentsCount(2);
        final OutputStreamWritable outputStreamWritable = v1FilterableIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 12);

        final Buffer byteBuffer = Buffer.from(os.toByteArray());

        final long fullSizeInBytes = byteBuffer.getLong();
        Assert.assertEquals(fullSizeInBytes, outputStreamWritable.getSizeInBytes());

        final int segmentTypeCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.FIXED_LENGTH_FILTER.getCode(), segmentTypeCode);

        final byte[] digest = calculateDigest(byteBuffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

        final int fieldNameLength = byteBuffer.getInt();
        Assert.assertEquals("fixed_length_field_name".getBytes().length, fieldNameLength);

        final byte[] fieldNameBuffer = new byte[fieldNameLength];
        byteBuffer.get(fieldNameBuffer);
        final String fieldName = new String(fieldNameBuffer);
        Assert.assertEquals("fixed_length_field_name", fieldName);

        //Reading values
        final long valuesSize = byteBuffer.getLong();
        Assert.assertEquals(52, valuesSize);

        final int elementSize = byteBuffer.getInt();
        Assert.assertEquals(4, elementSize);

        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(11, elementsCount);


        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[elementSize];
            byteBuffer.get(currentElementBytes);
            elements.add(UnsignedByteArrays.raw(currentElementBytes));
        }
        //elements - should equals to sorted collection of bytes (byteArraysDoc1 merged with byteArraysDoc2)
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{0, 1, 2, 3}), elements.get(0));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}), elements.get(1));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{4, 5, 6, 7}), elements.get(2));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{7, 6, 5, 4}), elements.get(3));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{7, 7, 7, 7}), elements.get(4));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{8, 9, 10, 11}), elements.get(5));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{11, 10, 9, 8}), elements.get(6));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{12, 13, 14, 15}), elements.get(7));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{15, 14, 13, 12}), elements.get(8));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{16, 17, 18, 19}), elements.get(9));
        Assert.assertEquals(UnsignedByteArrays.raw(new byte[]{19, 18, 17, 16}), elements.get(10));
        //Reading indexes
        final long indexesSize = byteBuffer.getLong();
        Assert.assertEquals(100, indexesSize);
        final int code = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode(), code);

//        final int valuesToDocumentKeysCount = byteBuffer.getInt();
//        Assert.assertEquals(2, valuesToDocumentKeysCount);
//        final int keysCount = byteBuffer.getInt();
//        Assert.assertEquals(11, keysCount);
//        final int[] indexOffsets = new int[keysCount];
//
//        for (int i = 0; i < keysCount; i++) {
//            final int currentOffset = byteBuffer.getInt();
//            indexOffsets[i] = currentOffset;
//        }
//        Assert.assertArrayEquals(new int[]{0, 8, 16, 24, 32, 44, 52, 60, 68, 76, 84}, indexOffsets);
//
//        final int[][] indexes = new int[11][];
//        for (int i = 0; i < keysCount; i++) {
//            final int currentSetSize = byteBuffer.getInt();
//            indexes[i] = new int[currentSetSize];
//            for (int j = 0; j < currentSetSize; j++) {
//                final int index = byteBuffer.getInt();
//                indexes[i][j] = index;
//            }
//        }
//
//        Assert.assertArrayEquals(new int[]{0}, indexes[0]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[1]);
//        Assert.assertArrayEquals(new int[]{0}, indexes[2]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[3]);
//        Assert.assertArrayEquals(new int[]{0, 1}, indexes[4]);
//        Assert.assertArrayEquals(new int[]{0}, indexes[5]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[6]);
//        Assert.assertArrayEquals(new int[]{0}, indexes[7]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[8]);
//        Assert.assertArrayEquals(new int[]{0}, indexes[9]);
//        Assert.assertArrayEquals(new int[]{1}, indexes[10]);
//
//        int digestSize = byteBuffer.getInt();
//        Assert.assertEquals(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES, digestSize);
//        for (int i = 0; i < digestSize; i++) {
//            byte actualDigestByte = byteBuffer.get();
//            Assert.assertEquals(digest[i], actualDigestByte);
//        }
//
//        //check that input has not remaining
//        Assert.assertFalse(byteBuffer.hasRemaining());
    }


}


