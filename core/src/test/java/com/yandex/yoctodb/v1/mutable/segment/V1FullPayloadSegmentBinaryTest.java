/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable.segment;

import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * @author svyatoslav
 */
public class V1FullPayloadSegmentBinaryTest {
    @Test
    public void writingPayloadTest() throws IOException {
        final Collection<UnsignedByteArray> payloads = new LinkedList<>();
        for (int i = 0; i < 15; i++) {
            final String payload = "payload" + i;
            payloads.add(UnsignedByteArrays.from(payload.getBytes()));
        }
        final V1FullPayloadSegment v1PayloadSegment =
                new V1FullPayloadSegment(payloads);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final OutputStreamWritable outputStreamWritable = v1PayloadSegment.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 12);

        final Buffer byteBuffer = Buffer.from(os.toByteArray());

        final long fullSizeInBytes = byteBuffer.getLong();
        Assert.assertEquals(fullSizeInBytes, outputStreamWritable.getSizeInBytes());

        final int payloadCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.PAYLOAD_FULL.getCode(), payloadCode);

        final long elementsSizeInBytes = byteBuffer.getLong();
        Assert.assertEquals(257, elementsSizeInBytes);

        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(15, elementsCount);

        final long[] offsets = new long[elementsCount + 1];

        for (int i = 0; i <= elementsCount; i++) {
            offsets[i] = byteBuffer.getLong();
        }
        Assert.assertArrayEquals(new long[]{0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 89, 98, 107, 116, 125}, offsets);

        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[(int) (offsets[i + 1] - offsets[i])];
            byteBuffer.get(currentElementBytes);
            Assert.assertEquals(("payload" + i), new String(currentElementBytes));
        }

        //check that input has not remaining
        Assert.assertFalse(byteBuffer.hasRemaining());
    }
}
