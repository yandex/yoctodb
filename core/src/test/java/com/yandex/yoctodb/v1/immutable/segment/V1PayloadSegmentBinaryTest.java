/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.immutable.Payload;
import com.yandex.yoctodb.util.OutputStreamWritable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author svyatoslav
 *         Date: 08.11.13
 */

public class V1PayloadSegmentBinaryTest {

    @Test
    public void readingWithSegmentRegistryTest() throws IOException {
        final Buffer byteBuffer = preparePayload();
        int fullSize = byteBuffer.getInt();
        //full size without 8 bytes (without 4 bytes for full_size_value and 4 bytes for segment_code value)
        Assert.assertEquals(fullSize + 4, byteBuffer.remaining());

        int code = byteBuffer.getInt();
        Payload payloadSegment = (Payload) SegmentRegistry.read(code, byteBuffer);

        for (int i = 0; i < 15; i++) {
            final Buffer currentPayload = payloadSegment.getPayload(i);
            final byte[] expectedPayloadBytes = ("payload" + i).getBytes();

            for (byte expectedByte : expectedPayloadBytes) {
                final byte actualByte = currentPayload.get();
                Assert.assertEquals(actualByte, expectedByte);
            }
            Assert.assertFalse(currentPayload.hasRemaining());

        }
    }


    private Buffer preparePayload() throws IOException {
        final com.yandex.yoctodb.v1.mutable.segment.V1PayloadSegment v1PayloadSegment =
                new com.yandex.yoctodb.v1.mutable.segment.V1PayloadSegment();

        for (int i = 0; i < 15; i++) {
            final byte[] payload = ("payload" + i).getBytes();
            v1PayloadSegment.addDocument(i, payload);
        }
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final OutputStreamWritable outputStreamWritable = v1PayloadSegment.buildWritable();
        outputStreamWritable.writeTo(os);
        return Buffer.wrap(os.toByteArray());
    }
}
