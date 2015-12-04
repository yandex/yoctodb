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

/**
 * @author svyatoslav
 */
public class V1FullIndexBinaryTest {

    @Test
    public void writingFixedLengthFullIndex() throws IOException {
        final String fieldName = "fixed_length_field_name";
        V1FullIndex v1FullIndex = new V1FullIndex(fieldName, true);

        //first doc
        Collection<UnsignedByteArray> byteArraysDoc1 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc1.add(UnsignedByteArrays.from(new byte[]{7, 7, 7, 7}));
        v1FullIndex.addDocument(0, byteArraysDoc1);

        //second doc
        Collection<UnsignedByteArray> byteArraysDoc2 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc2.add(UnsignedByteArrays.from(new byte[]{3, 2, 1, 0}));
        v1FullIndex.addDocument(1, byteArraysDoc2);

        //third doc
        Collection<UnsignedByteArray> byteArraysDoc3 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc3.add(UnsignedByteArrays.from(new byte[]{7, 7, 7, 7}));
        v1FullIndex.addDocument(2, byteArraysDoc3);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        v1FullIndex.setDatabaseDocumentsCount(3);
        OutputStreamWritable outputStreamWritable = v1FullIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 12);

        Buffer byteBuffer = Buffer.from(os.toByteArray());

        final long fullSizeInBytes = byteBuffer.getLong();
        Assert.assertEquals(fullSizeInBytes, outputStreamWritable.getSizeInBytes());

        int segmentCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.FIXED_LENGTH_FULL_INDEX.getCode(), segmentCode);

        final int fieldNameLength = byteBuffer.getInt();
        Assert.assertEquals(fieldName.getBytes().length, fieldNameLength);

        final byte[] fieldNameBuffer = new byte[fieldNameLength];
        byteBuffer.get(fieldNameBuffer);
        Assert.assertEquals(fieldName, new String(fieldNameBuffer));

        //Reading values
        final long valuesSize = byteBuffer.getLong();
        Assert.assertEquals(16, valuesSize);

        final int elementSize = byteBuffer.getInt();
        Assert.assertEquals(4, elementSize);

        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(2, elementsCount);

        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[elementSize];
            byteBuffer.get(currentElementBytes);
            elements.add(UnsignedByteArrays.from(currentElementBytes));
        }
        //elements - should equals to sorted collection of bytes (byteArraysDoc1 merged with byteArraysDoc2)
        Assert.assertEquals(
                UnsignedByteArrays.from(new byte[]{3, 2, 1, 0}),
                elements.get(0));
        Assert.assertEquals(
                UnsignedByteArrays.from(new byte[]{7, 7, 7, 7}),
                elements.get(1));

        //Reading values to document indexes
        final long valuesToDocumentIndexesSize1 = byteBuffer.getLong();
        Assert.assertEquals(44, valuesToDocumentIndexesSize1);
        final int code = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.MultiMapType.LIST_BASED.getCode(), code);
        final int valuesToDocumentKeysCount = byteBuffer.getInt();
        Assert.assertEquals(2, valuesToDocumentKeysCount);
        final long[] documentToValueIndexOffsets = new long[valuesToDocumentKeysCount];

        for (int i = 0; i < valuesToDocumentKeysCount; i++) {
            final long currentOffset = byteBuffer.getLong();
            documentToValueIndexOffsets[i] = currentOffset;
        }
        Assert.assertArrayEquals(new long[]{0, 8}, documentToValueIndexOffsets);

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
        final long documentsToValueIndexSizeInBytes = byteBuffer.getLong();
        Assert.assertEquals(16, documentsToValueIndexSizeInBytes);

        final int documentsToValueKeysCount = byteBuffer.getInt();
        Assert.assertEquals(3, documentsToValueKeysCount);
        final int[] documentsToValueIndex = new int[documentsToValueKeysCount];

        for (int i = 0; i < documentsToValueKeysCount; i++) {
            int valueId = byteBuffer.getInt();
            documentsToValueIndex[i] = valueId;
        }
        Assert.assertArrayEquals(new int[]{1, 0, 1}, documentsToValueIndex);

        //check that input has not remaining
        Assert.assertFalse(byteBuffer.hasRemaining());
    }

    @Test
    public void writingVariableLengthFullIndex() throws IOException {
        final V1FullIndex v1FullIndex = new V1FullIndex("variable_length_field_name", false);

        //first doc
        final Collection<UnsignedByteArray> byteArraysDoc1 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc1.add(UnsignedByteArrays.from(new byte[]{7, 7, 7, 7, 7}));
        v1FullIndex.addDocument(0, byteArraysDoc1);

        //second doc
        final Collection<UnsignedByteArray> byteArraysDoc2 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc2.add(UnsignedByteArrays.from(new byte[]{3, 2, 1, 0}));
        v1FullIndex.addDocument(1, byteArraysDoc2);

        //third doc
        final Collection<UnsignedByteArray> byteArraysDoc3 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc3.add(UnsignedByteArrays.from(new byte[]{7, 7, 7, 7, 7}));
        v1FullIndex.addDocument(2, byteArraysDoc3);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        v1FullIndex.setDatabaseDocumentsCount(3);
        OutputStreamWritable outputStreamWritable = v1FullIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 12);

        final Buffer byteBuffer = Buffer.from(os.toByteArray());

        final long fullSizeInBytes = byteBuffer.getLong();

        Assert.assertEquals(fullSizeInBytes, outputStreamWritable.getSizeInBytes());

        final int segmentCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.VARIABLE_LENGTH_FULL_INDEX.getCode(), segmentCode);

        final int fieldNameLength = byteBuffer.getInt();
        Assert.assertEquals("variable_length_field_name".getBytes().length, fieldNameLength);

        final byte[] fieldNameBuffer = new byte[fieldNameLength];
        byteBuffer.get(fieldNameBuffer);
        final String fieldName = new String(fieldNameBuffer);
        Assert.assertEquals("variable_length_field_name", fieldName);

        //Reading values
        final long valuesSize = byteBuffer.getLong();
        Assert.assertEquals(37, valuesSize);

        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(2, elementsCount);

        final long[] elementOffsets = new long[elementsCount + 1];

        for (int i = 0; i <= elementsCount; i++) {
            final long currentOffset = byteBuffer.getLong();
            elementOffsets[i] = currentOffset;
        }

        Assert.assertArrayEquals(new long[]{0, 4, 9}, elementOffsets);

        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[(int) elementOffsets[i + 1] - (int) elementOffsets[i]];
            byteBuffer.get(currentElementBytes);
            elements.add(UnsignedByteArrays.from(currentElementBytes));
        }
        //elements - should equals to sorted collection of bytes (byteArraysDoc1 merged with byteArraysDoc2)
        Assert.assertEquals(
                elements.get(0),
                UnsignedByteArrays.from(new byte[]{3, 2, 1, 0}));
        Assert.assertEquals(
                elements.get(1),
                UnsignedByteArrays.from(new byte[]{7, 7, 7, 7, 7}));

        //Reading values to document indexes
        final long valuesToDocumentIndexesSize1 = byteBuffer.getLong();
        Assert.assertEquals(44, valuesToDocumentIndexesSize1);
        final int code = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.MultiMapType.LIST_BASED.getCode(), code);
        final int valuesToDocumentKeysCount = byteBuffer.getInt();
        Assert.assertEquals(2, valuesToDocumentKeysCount);
        final long[] documentToValueIndexOffsets = new long[valuesToDocumentKeysCount];

        for (int i = 0; i < valuesToDocumentKeysCount; i++) {
            final long currentOffset = byteBuffer.getLong();
            documentToValueIndexOffsets[i] = currentOffset;
        }
        Assert.assertArrayEquals(new long[]{0, 8}, documentToValueIndexOffsets);

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
        final long documentsToValueIndexSizeInBytes = byteBuffer.getLong();
        Assert.assertEquals(16, documentsToValueIndexSizeInBytes);

        final int documentsToValueKeysCount = byteBuffer.getInt();
        Assert.assertEquals(3, documentsToValueKeysCount);
        final int[] documentsToValueIndex = new int[documentsToValueKeysCount];

        for (int i = 0; i < documentsToValueKeysCount; i++) {
            final int valueId = byteBuffer.getInt();
            documentsToValueIndex[i] = valueId;
        }
        Assert.assertArrayEquals(new int[]{1, 0, 1}, documentsToValueIndex);

        //check that input has not remaining
        Assert.assertFalse(byteBuffer.hasRemaining());
    }


}
