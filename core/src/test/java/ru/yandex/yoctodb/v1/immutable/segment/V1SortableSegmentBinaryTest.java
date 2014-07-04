package ru.yandex.yoctodb.v1.immutable.segment;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.yoctodb.immutable.SortableIndex;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.UnsignedByteArrays;
import ru.yandex.yoctodb.util.OutputStreamWritable;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;
import ru.yandex.yoctodb.v1.mutable.segment.V1SortableIndex;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author svyatoslav
 *         Date: 08.12.13
 */
public class V1SortableSegmentBinaryTest {

    @Test
    public void readingTestFixedLength() throws IOException {
        final ByteBuffer byteBuffer = prepareFixedLengthSegment();
        int fullSize = byteBuffer.getInt();
        Assert.assertEquals(4046, fullSize);
        final int segmentCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.FIXED_LENGTH_SORT.getCode(), segmentCode);
        Segment segment = SegmentRegistry.read(V1DatabaseFormat.SegmentType.FIXED_LENGTH_SORT.getCode(), byteBuffer);
        SortableIndex si = (SortableIndex) segment;
        Assert.assertEquals("field_name", si.getFieldName());
    }

    private ByteBuffer prepareFixedLengthSegment() throws IOException {
        V1SortableIndex sortableIndex = new V1SortableIndex("field_name", true);
        for (int i = 1000; i < 2000; i++) {
            List<UnsignedByteArray> values = new ArrayList<UnsignedByteArray>();

            values.add(UnsignedByteArrays.from(Integer.toString(i)));

            sortableIndex.addDocument(i - 1000, values);
        }
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final OutputStreamWritable outputStreamWritable = sortableIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        return ByteBuffer.wrap(os.toByteArray());
    }

    @Test
    public void readingTestVariableLength() throws IOException {
        final ByteBuffer byteBuffer = prepareVariableLengthSegment();
        int fullSize = byteBuffer.getInt();
        Assert.assertEquals(10936, fullSize);
        final int segmentCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.VARIABLE_LENGTH_SORT.getCode(), segmentCode);
        Segment segment = SegmentRegistry.read(V1DatabaseFormat.SegmentType.VARIABLE_LENGTH_SORT.getCode(), byteBuffer);
        SortableIndex si = (SortableIndex) segment;
        Assert.assertEquals("field_name", si.getFieldName());
    }

    private ByteBuffer prepareVariableLengthSegment() throws IOException {
        V1SortableIndex sortableIndex = new V1SortableIndex("field_name", false);
        for (int i = 0; i < 1000; i++) {
            List<UnsignedByteArray> values = new ArrayList<UnsignedByteArray>();
            values.add(UnsignedByteArrays.from("text" + i));

            sortableIndex.addDocument(i, values);
        }
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final OutputStreamWritable outputStreamWritable = sortableIndex.buildWritable();
        outputStreamWritable.writeTo(os);
        return ByteBuffer.wrap(os.toByteArray());
    }
}
