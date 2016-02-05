/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.immutable.Payload;
import com.yandex.yoctodb.util.OutputStreamWritable;

import java.io.*;

/**
 * @author svyatoslav
 */
public class V1PayloadSegmentBinaryTest {

    @Test
    public void readingWithSegmentRegistryTest() throws IOException {
        final Buffer byteBuffer = preparePayload();
        final long fullSize = byteBuffer.getLong();
        // full size without 4 bytes for segment_code value
        Assert.assertEquals(fullSize + 4, byteBuffer.remaining());

        final int code = byteBuffer.getInt();
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

    @Test
    public void hugePayload() throws IOException {
        final com.yandex.yoctodb.v1.mutable.segment.V1PayloadSegment v1PayloadSegment =
                new com.yandex.yoctodb.v1.mutable.segment.V1PayloadSegment();

        final byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < payload.length; i++)
            payload[i] = (byte) i;

        final int docs = 4 * 1024;
        for (int i = 0; i < docs; i++) {
            v1PayloadSegment.addDocument(i, payload);
        }

        final OutputStreamWritable outputStreamWritable =
                v1PayloadSegment.buildWritable();

        final File f = File.createTempFile("huge", "dat");
        f.deleteOnExit();

        final OutputStream os =
                new BufferedOutputStream(
                        new FileOutputStream(f));
        try {
            outputStreamWritable.writeTo(os);
        } finally {
            os.close();
        }

        final Buffer buf =
                Buffer.from(new RandomAccessFile(f, "r").getChannel());

        final long segmentSize = buf.getLong();
        Assert.assertEquals(segmentSize + 4, buf.remaining());

        final int code = buf.getInt();
        final Payload segment = (Payload) SegmentRegistry.read(code, buf);

        final Buffer expectedPayload = Buffer.from(payload);
        for (int i = 0; i < docs; i += docs >> 2) {
            final Buffer currentPayload = segment.getPayload(i);
            Assert.assertEquals(expectedPayload, currentPayload);
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
        return Buffer.from(os.toByteArray());
    }
}
