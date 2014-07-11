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
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.yandex.yoctodb.v1.mutable.segment.Utils.calculateDigest;

/**
 * @author svyatoslav
 *         Date: 08.11.13
 */
public class V1PayloadSegmentBinaryTest {

    @Test
    public void writingPayloadTest() throws IOException {
        final V1PayloadSegment v1PayloadSegment = new V1PayloadSegment();
        for (int i = 0; i < 15; i++) {
            final String payload = "payload" + i;
            v1PayloadSegment.addDocument(i, payload.getBytes());
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final OutputStreamWritable outputStreamWritable = v1PayloadSegment.buildWritable();
        outputStreamWritable.writeTo(os);
        Assert.assertEquals(os.size(), outputStreamWritable.getSizeInBytes() + 8);

        final ByteBuffer byteBuffer = ByteBuffer.wrap(os.toByteArray());

        final int fullSizeInBytes = byteBuffer.getInt();
        Assert.assertEquals(fullSizeInBytes, outputStreamWritable.getSizeInBytes());

        final int payloadCode = byteBuffer.getInt();
        Assert.assertEquals(V1DatabaseFormat.SegmentType.PAYLOAD.getCode(), payloadCode);

        final byte[] digest = calculateDigest(byteBuffer, V1DatabaseFormat.MESSAGE_DIGEST_ALGORITHM);

        final int elementsSizeInButes = byteBuffer.getInt();
        Assert.assertEquals(193, elementsSizeInButes);

        final int elementsCount = byteBuffer.getInt();
        Assert.assertEquals(15, elementsCount);

        final int[] offsets = new int[elementsCount + 1];

        for (int i = 0; i <= elementsCount; i++) {
            offsets[i] = byteBuffer.getInt();
        }
        Assert.assertArrayEquals(new int[]{0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 89, 98, 107, 116, 125}, offsets);

        for (int i = 0; i < elementsCount; i++) {
            final byte[] currentElementBytes = new byte[offsets[i + 1] - offsets[i]];
            byteBuffer.get(currentElementBytes);
            Assert.assertEquals(("payload" + i), new String(currentElementBytes));
        }

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
