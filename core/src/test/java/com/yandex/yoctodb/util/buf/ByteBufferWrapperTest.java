/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.buf;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link com.yandex.yoctodb.util.buf.ByteBufferWrapper}
 *
 * @author dimas
 */
public class ByteBufferWrapperTest extends BufferTest {
    @Override
    protected Buffer bufferOf(byte[] data) {
        return Buffer.from(data);
    }

    @Test
    public void testGetShort() {
        short data = 32765;
        byte[] bytes = new byte[]{
                (byte) ((data >> 8) & 0xff),
                (byte) ((data) & 0xff)
        };
        final Buffer buf = Buffer.from(bytes);
        short result = buf.getShort();
        assertEquals(data, result);
    }

    @Test
    public void testGetShortWithIndex() {
        short data = 32765;
        byte[] bytes = new byte[]{
                (byte) ((data >> 8) & 0xff),
                (byte) ((data) & 0xff)
        };
        final Buffer buf = Buffer.from(bytes);
        short result = buf.getShort(0);
        assertEquals(data, result);
    }
}
