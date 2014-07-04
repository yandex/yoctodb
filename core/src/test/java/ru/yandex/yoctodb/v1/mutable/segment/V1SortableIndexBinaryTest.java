package ru.yandex.yoctodb.v1.mutable.segment;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.UnsignedByteArrays;
import ru.yandex.yoctodb.util.OutputStreamWritable;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static ru.yandex.yoctodb.v1.mutable.segment.Utils.calculateDigest;

/**
 * @author svyatoslav
 *         Date: 08.11.13
 */
public class V1SortableIndexBinaryTest {

    @Test
    public void writeVariableLengthSortableIndex() throws IOException {
        final V1SortableIndex v1SortableIndex = new V1SortableIndex("variable_length_field_name", false);
        //first doc
        final Collection<UnsignedByteArray> byteArraysDoc1 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{1, 2, 3, 4, 5}));
        v1SortableIndex.addDocument(0, byteArraysDoc1);

        //second doc
        final Collection<UnsignedByteArray> byteArraysDoc2 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}));
        v1SortableIndex.addDocument(1, byteArraysDoc2);

        //third doc
        final Collection<UnsignedByteArray> byteArraysDoc3 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc3.add(UnsignedByteArrays.raw(new byte[]{1, 2, 3, 4, 5}));
        v1SortableIndex.addDocument(2, byteArraysDoc3);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final OutputStreamWritable outputStreamWritable = v1SortableIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 8);

        final ByteBuffer byteBuffer = ByteBuffer.wrap(os.toByteArray());
        final int fullSizeInBytes = byteBuffer.getInt();
        Assert.assertEquals(outputStreamWritable.getSizeInBytes(), fullSizeInBytes);

        final int segmentCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.VARIABLE_LENGTH_SORT.getCode(), segmentCode);

        final byte[] digest = calculateDigest(byteBuffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

        final int fieldNameLength = byteBuffer.getInt();
        Assert.assertEquals("variable_length_field_name".getBytes().length, fieldNameLength);

        final byte[] fieldNameBuffer = new byte[fieldNameLength];
        byteBuffer.get(fieldNameBuffer);
        final String fieldName = new String(fieldNameBuffer);
        Assert.assertEquals("variable_length_field_name", fieldName);

        // Reading length
        Assert.assertEquals(34, byteBuffer.getInt());

        //Reading values
        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(3, elementsCount);

        final int[] elementOffsets = new int[elementsCount + 1];

        for (int i = 0; i <= elementsCount; i++) {
            final int currentOffset = byteBuffer.getInt();
            elementOffsets[i] = currentOffset;
        }

        Assert.assertArrayEquals(new int[]{0, 5, 9, 14}, elementOffsets);

        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[elementOffsets[i + 1] - elementOffsets[i]];
            byteBuffer.get(currentElementBytes);
            elements.add(UnsignedByteArrays.raw(currentElementBytes));
        }

        Assert.assertEquals(
                UnsignedByteArrays.raw(new byte[]{1, 2, 3, 4, 5}),
                elements.get(0));
        Assert.assertEquals(
                UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}),
                elements.get(1));
        Assert.assertEquals(
                UnsignedByteArrays.raw(new byte[]{1, 2, 3, 4, 5}),
                elements.get(2));

        int digestSize = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.DIGEST_SIZE_IN_BYTES, digestSize);
        for (int i = 0; i < digestSize; i++) {
            byte actualDigestByte = byteBuffer.get();
            Assert.assertEquals(digest[i], actualDigestByte);
            Assert.assertEquals(digest[i], actualDigestByte);
        }

        //check that input has not remaining
        Assert.assertFalse(byteBuffer.hasRemaining());
    }

    @Test
    public void writeFixedLengthSortableIndex() throws IOException {
        final V1SortableIndex v1SortableIndex = new V1SortableIndex("fixed_length_field_name", true);
        //first doc
        final Collection<UnsignedByteArray> byteArraysDoc1 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc1.add(UnsignedByteArrays.raw(new byte[]{1, 2, 3, 4}));
        v1SortableIndex.addDocument(0, byteArraysDoc1);

        //second doc
        final Collection<UnsignedByteArray> byteArraysDoc2 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc2.add(UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}));
        v1SortableIndex.addDocument(1, byteArraysDoc2);

        //third doc
        final Collection<UnsignedByteArray> byteArraysDoc3 = new ArrayList<UnsignedByteArray>();
        byteArraysDoc3.add(UnsignedByteArrays.raw(new byte[]{1, 2, 3, 4}));
        v1SortableIndex.addDocument(2, byteArraysDoc3);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final OutputStreamWritable outputStreamWritable = v1SortableIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 8);

        final ByteBuffer byteBuffer = ByteBuffer.wrap(os.toByteArray());

        final int fullSizeInBytes = byteBuffer.getInt();
        Assert.assertEquals(outputStreamWritable.getSizeInBytes(), fullSizeInBytes);

        final int segmentCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.FIXED_LENGTH_SORT.getCode(), segmentCode);

        final byte[] digest = calculateDigest(byteBuffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

        final int fieldNameLength = byteBuffer.getInt();
        Assert.assertEquals("fixed_length_field_name".getBytes().length, fieldNameLength);

        final byte[] fieldNameBuffer = new byte[fieldNameLength];
        byteBuffer.get(fieldNameBuffer);
        final String fieldName = new String(fieldNameBuffer);
        Assert.assertEquals("fixed_length_field_name", fieldName);

        // Reading length
        Assert.assertEquals(20, byteBuffer.getInt());

        //Reading values
        final int elementSize = byteBuffer.getInt();
        Assert.assertEquals(4, elementSize);

        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(3, elementsCount);


        final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();
        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[elementSize];
            byteBuffer.get(currentElementBytes);
            elements.add(UnsignedByteArrays.raw(currentElementBytes));
        }
        //elements - should equals to sorted collection of bytes (byteArraysDoc1 merged with byteArraysDoc2)
        Assert.assertEquals(
                UnsignedByteArrays.raw(new byte[]{1, 2, 3, 4}),
                elements.get(0));
        Assert.assertEquals(
                UnsignedByteArrays.raw(new byte[]{3, 2, 1, 0}),
                elements.get(1));
        Assert.assertEquals(
                UnsignedByteArrays.raw(new byte[]{1, 2, 3, 4}),
                elements.get(2));

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
