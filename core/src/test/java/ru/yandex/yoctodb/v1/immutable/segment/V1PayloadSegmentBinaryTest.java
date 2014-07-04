package ru.yandex.yoctodb.v1.immutable.segment;

import org.junit.Assert;
import org.junit.Test;
import ru.yandex.yoctodb.immutable.Payload;
import ru.yandex.yoctodb.util.OutputStreamWritable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author svyatoslav
 *         Date: 08.11.13
 */

public class V1PayloadSegmentBinaryTest {

    @Test
    public void readingWithSegmentRegistryTest() throws IOException {
        final ByteBuffer byteBuffer = preparePayload();
        int fullSize = byteBuffer.getInt();
        //full size without 8 bytes (without 4 bytes for full_size_value and 4 bytes for segment_code value)
        Assert.assertEquals(fullSize + 8, byteBuffer.limit());

        int code = byteBuffer.getInt();
        Payload payloadSegment = (Payload) SegmentRegistry.read(code, byteBuffer);

        for (int i = 0; i < 15; i++) {
            final ByteBuffer currentPayload = payloadSegment.getPayload(i);
            final byte[] expectedPayloadBytes = ("payload" + i).getBytes();

            for (byte expectedByte : expectedPayloadBytes) {
                final byte actualByte = currentPayload.get();
                Assert.assertEquals(actualByte, expectedByte);
            }
            Assert.assertFalse(currentPayload.hasRemaining());

        }
    }


    private ByteBuffer preparePayload() throws IOException {
        final ru.yandex.yoctodb.v1.mutable.segment.V1PayloadSegment v1PayloadSegment =
                new ru.yandex.yoctodb.v1.mutable.segment.V1PayloadSegment();

        for (int i = 0; i < 15; i++) {
            final byte[] payload = ("payload" + i).getBytes();
            v1PayloadSegment.addDocument(i, payload);
        }
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final OutputStreamWritable outputStreamWritable = v1PayloadSegment.buildWritable();
        outputStreamWritable.writeTo(os);
        return ByteBuffer.wrap(os.toByteArray());
    }
}
