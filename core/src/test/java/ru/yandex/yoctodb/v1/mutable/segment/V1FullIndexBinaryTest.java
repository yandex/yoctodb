/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.v1.mutable.segment;

import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.yandex.yoctodb.v1.mutable.segment.Utils.calculateDigest;

/**
 * @author svyatoslav
 *         Date: 08.11.13
 */
public class V1FullIndexBinaryTest {

    @Test
    public void writingFixedLengthFullIndex() throws IOException {
        V1FullIndex v1FullIndex = new V1FullIndex("fixed_length_field_name", true);

        //first doc
        Collection<UnsignedByteArray> byteArraysDoc1 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{7, 7, 7, 7}));
        v1FullIndex.addDocument(0, byteArraysDoc1);

        //second doc
        Collection<UnsignedByteArray> byteArraysDoc2 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}));
        v1FullIndex.addDocument(1, byteArraysDoc2);

        //third doc
        Collection<UnsignedByteArray> byteArraysDoc3 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc3.add(UnsignedByteArrays.raw(new byte[]{7, 7, 7, 7}));
        v1FullIndex.addDocument(2, byteArraysDoc3);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        v1FullIndex.setDatabaseDocumentsCount(3);
        OutputStreamWritable outputStreamWritable = v1FullIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 8);

        ByteBuffer byteBuffer = ByteBuffer.wrap(os.toByteArray());

        final int fullSizeInBytes = byteBuffer.getInt();
        Assert.assertEquals(fullSizeInBytes, outputStreamWritable.getSizeInBytes());

        int segmentCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.FIXED_LENGTH_FULL_INDEX.getCode(), segmentCode);

        final byte[] digest = calculateDigest(byteBuffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

        final int fieldNameLength = byteBuffer.getInt();
        Assert.assertEquals("fixed_length_field_name".getBytes().length, fieldNameLength);

        final byte[] fieldNameBuffer = new byte[fieldNameLength];
        byteBuffer.get(fieldNameBuffer);
        final String fieldName = new String(fieldNameBuffer);
        Assert.assertEquals("fixed_length_field_name", fieldName);


        //Reading values
        final int valuesSize = byteBuffer.getInt();
        Assert.assertEquals(16, valuesSize);

        final int elementSize = byteBuffer.getInt();
        Assert.assertEquals(4, elementSize);

        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(2, elementsCount);


        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[elementSize];
            byteBuffer.get(currentElementBytes);
            elements.add(UnsignedByteArrays.raw(currentElementBytes));
        }
        //elements - should equals to sorted collection of bytes (byteArraysDoc1 merged with byteArraysDoc2)
        Assert.assertEquals(
                UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}),
                elements.get(0));
        Assert.assertEquals(
                UnsignedByteArrays.raw(new byte[]{7, 7, 7, 7}),
                elements.get(1));

        //Reading values to document indexes
        final int valuesToDocumentIndexesSize1 = byteBuffer.getInt();
        Assert.assertEquals(36, valuesToDocumentIndexesSize1);
        final int code = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.MultiMapType.LIST_BASED.getCode(), code);
        final int valuesToDocumentKeysCount = byteBuffer.getInt();
        Assert.assertEquals(2, valuesToDocumentKeysCount);
        final int[] documentToValueIndexOffsets = new int[valuesToDocumentKeysCount];

        for (int i = 0; i < valuesToDocumentKeysCount; i++) {
            final int currentOffset = byteBuffer.getInt();
            documentToValueIndexOffsets[i] = currentOffset;
        }
        Assert.assertArrayEquals(new int[]{0, 8}, documentToValueIndexOffsets);

        final int[][] valuesToDocumentIndexes = new int[elementsCount][];
        for (int i = 0; i < valuesToDocumentKeysCount; i++) {
            final int currentSetSize = byteBuffer.getInt();
            valuesToDocumentIndexes[i] = new int[currentSetSize];
            for (int j = 0; j < currentSetSize; j++) {
                final int index = byteBuffer.getInt();
                valuesToDocumentIndexes[i][j] = index;
            }
        }

        Assert.assertArrayEquals(new int[]{1}, valuesToDocumentIndexes[0]);
        Assert.assertArrayEquals(new int[]{0, 2}, valuesToDocumentIndexes[1]);

        //Reading documents to value indexes
        final int documentsToValueIndexSizeInBytes = byteBuffer.getInt();
        Assert.assertEquals(16, documentsToValueIndexSizeInBytes);

        final int documentsToValueKeysCount = byteBuffer.getInt();
        Assert.assertEquals(3, documentsToValueKeysCount);
        final int[] documentsToValueIndex = new int[documentsToValueKeysCount];

        for (int i = 0; i < documentsToValueKeysCount; i++) {
            int valueId = byteBuffer.getInt();
            documentsToValueIndex[i] = valueId;
        }
        Assert.assertArrayEquals(new int[]{1, 0, 1}, documentsToValueIndex);

        int digestSize = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES, digestSize);
        for (int i = 0; i < digestSize; i++) {
            byte actualDigestByte = byteBuffer.get();
            Assert.assertEquals(digest[i], actualDigestByte);
        }

        //check that input has not remaining
        Assert.assertFalse(byteBuffer.hasRemaining());
    }

    @Test
    public void writingVariableLengthFullIndex() throws IOException {
        final V1FullIndex v1FullIndex = new V1FullIndex("variable_length_field_name", false);

        //first doc
        final Collection<UnsignedByteArray> byteArraysDoc1 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{7, 7, 7, 7, 7}));
        v1FullIndex.addDocument(0, byteArraysDoc1);

        //second doc
        final Collection<UnsignedByteArray> byteArraysDoc2 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}));
        v1FullIndex.addDocument(1, byteArraysDoc2);

        //third doc
        final Collection<UnsignedByteArray> byteArraysDoc3 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc3.add(UnsignedByteArrays.raw(new byte[]{7, 7, 7, 7, 7}));
        v1FullIndex.addDocument(2, byteArraysDoc3);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        v1FullIndex.setDatabaseDocumentsCount(3);
        OutputStreamWritable outputStreamWritable = v1FullIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 8);

        final ByteBuffer byteBuffer = ByteBuffer.wrap(os.toByteArray());

        final int fullSizeInBytes = byteBuffer.getInt();

        Assert.assertEquals(fullSizeInBytes, outputStreamWritable.getSizeInBytes());

        final int segmentCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.VARIABLE_LENGTH_FULL_INDEX.getCode(), segmentCode);

        final byte[] digest = calculateDigest(byteBuffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

        final int fieldNameLength = byteBuffer.getInt();
        Assert.assertEquals("variable_length_field_name".getBytes().length, fieldNameLength);

        final byte[] fieldNameBuffer = new byte[fieldNameLength];
        byteBuffer.get(fieldNameBuffer);
        final String fieldName = new String(fieldNameBuffer);
        Assert.assertEquals("variable_length_field_name", fieldName);

        //Reading values
        final int valuesSize = byteBuffer.getInt();
        Assert.assertEquals(29, valuesSize);

        final int maxElement = byteBuffer.getInt();
        Assert.assertEquals(5, maxElement);

        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(2, elementsCount);

        final int[] elementOffsets = new int[elementsCount + 1];

        for (int i = 0; i <= elementsCount; i++) {
            final int currentOffset = byteBuffer.getInt();
            elementOffsets[i] = currentOffset;
        }

        Assert.assertArrayEquals(new int[]{0, 4, 9}, elementOffsets);

        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[elementOffsets[i + 1] - elementOffsets[i]];
            byteBuffer.get(currentElementBytes);
            elements.add(UnsignedByteArrays.raw(currentElementBytes));
        }
        //elements - should equals to sorted collection of bytes (byteArraysDoc1 merged with byteArraysDoc2)
        Assert.assertEquals(
                elements.get(0),
                UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}));
        Assert.assertEquals(
                elements.get(1),
                UnsignedByteArrays.raw(new byte[]{7, 7, 7, 7, 7}));

        //Reading values to document indexes
        final int valuesToDocumentIndexesSize1 = byteBuffer.getInt();
        Assert.assertEquals(36, valuesToDocumentIndexesSize1);
        final int code = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.MultiMapType.LIST_BASED.getCode(), code);
        final int valuesToDocumentKeysCount = byteBuffer.getInt();
        Assert.assertEquals(2, valuesToDocumentKeysCount);
        final int[] documentToValueIndexOffsets = new int[valuesToDocumentKeysCount];

        for (int i = 0; i < valuesToDocumentKeysCount; i++) {
            final int currentOffset = byteBuffer.getInt();
            documentToValueIndexOffsets[i] = currentOffset;
        }
        Assert.assertArrayEquals(new int[]{0, 8}, documentToValueIndexOffsets);

        final int[][] valuesToDocumentIndexes = new int[elementsCount][];
        for (int i = 0; i < valuesToDocumentKeysCount; i++) {
            final int currentSetSize = byteBuffer.getInt();
            valuesToDocumentIndexes[i] = new int[currentSetSize];
            for (int j = 0; j < currentSetSize; j++) {
                final int index = byteBuffer.getInt();
                valuesToDocumentIndexes[i][j] = index;
            }
        }

        Assert.assertArrayEquals(new int[]{1}, valuesToDocumentIndexes[0]);
        Assert.assertArrayEquals(new int[]{0, 2}, valuesToDocumentIndexes[1]);

        //Reading documents to value indexes
        final int documentsToValueIndexSizeInBytes = byteBuffer.getInt();
        Assert.assertEquals(16, documentsToValueIndexSizeInBytes);

        final int documentsToValueKeysCount = byteBuffer.getInt();
        Assert.assertEquals(3, documentsToValueKeysCount);
        final int[] documentsToValueIndex = new int[documentsToValueKeysCount];

        for (int i = 0; i < documentsToValueKeysCount; i++) {
            final int valueId = byteBuffer.getInt();
            documentsToValueIndex[i] = valueId;
        }
        Assert.assertArrayEquals(new int[]{1, 0, 1}, documentsToValueIndex);


        int digestSize = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES, digestSize);
        for (int i = 0; i < digestSize; i++) {
            byte actualDigestByte = byteBuffer.get();
            Assert.assertEquals(digest[i], actualDigestByte);
        }

        //check that input has not remaining
        Assert.assertFalse(byteBuffer.hasRemaining());
    }


}
