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

import com.google.common.base.Charsets;
import com.google.common.primitives.Shorts;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.yandex.yoctodb.util.buf.Buffer}
 *
 * @author dimas
 */
public abstract class BufferTest {

    private static final String ALPHABET =
            "abcdefghijklmnopqrstuvwxyz";
    private static final byte[] ALPHABET_BYTES = ALPHABET.
            getBytes(Charsets.US_ASCII);

    protected abstract Buffer bufferOf(byte[] data);

    protected Buffer bufferOf(String s) {
        return bufferOf(s.getBytes(Charsets.US_ASCII));
    }

    @Test
    public void testEqual() {
        final Buffer buf1 = bufferOf("superMegaBuffer");
        assertEquals(buf1, buf1);
        final Buffer buf2 = bufferOf("superMegaBuffer");
        assertEquals(buf1.hashCode(), buf2.hashCode());
        assertEquals(buf1, buf2);
    }

    @Test
    public void testNotEqual() {
        final Buffer buf1 = bufferOf("superMegaBuffer1");
        assertNotEquals(buf1, "Some string");
        final Buffer buf2 = bufferOf("superMegaBuffer2");
        assertNotEquals(buf1.hashCode(), buf2.hashCode());
        assertNotEquals(buf1, buf2);
        assertNotEquals(bufferOf("short"), bufferOf("long"));
    }

    @Test
    public void testPosition() {
        final Buffer buf = bufferOf(ALPHABET);
        assertEquals(0, buf.position());
        assertEquals(1, buf.advance(1).position());
        assertTrue(buf.hasRemaining());

        assertFalse(buf.position(ALPHABET.length()).hasRemaining());
    }

    @Test
    public void testGet() {
        final Buffer buf = bufferOf(ALPHABET);
        assertEquals('a', (char) buf.get());
        assertEquals('b', (char) buf.get());
        assertEquals(2, buf.position());
    }

    @Test
    public void testAbsoluteGet() {
        final Buffer buf = bufferOf(ALPHABET);
        assertEquals('d', (char) buf.get(3));
        assertEquals(0, buf.position());
    }

    @Test
    public void testGetToBuffer() {
        final Buffer buf = bufferOf(ALPHABET);
        final byte[] bytes = new byte[ALPHABET_BYTES.length];

        buf.get(bytes);

        assertEquals(buf.position(), ALPHABET.length());
        assertFalse(buf.hasRemaining());
        assertEquals(
                ByteBuffer.wrap(ALPHABET_BYTES),
                ByteBuffer.wrap(bytes, 0, ALPHABET_BYTES.length));
    }

    @Test
    public void testGetToBufferWithOffset() {
        final Buffer buf = bufferOf(ALPHABET);
        final byte[] bytes = new byte[1024];

        buf.get(bytes, 128, 10);

        assertEquals(buf.position(), 10);
        assertTrue(buf.hasRemaining());
        assertEquals(
                ByteBuffer.wrap(ALPHABET_BYTES, 0, 10),
                ByteBuffer.wrap(bytes, 128, 10));
    }

    @Test
    public void testGetShort() {
        short data = 32765;
        byte[] bytes = Shorts.toByteArray(data);
        final Buffer buf = Buffer.from(bytes);
        short result = buf.getShort();
        assertEquals(data, result);
    }

    @Test
    public void testGetShortWithIndex() {
        short data = 32765;
        byte[] bytes = Shorts.toByteArray(data);
        final Buffer buf = Buffer.from(bytes);
        short result = buf.getShort(0);
        assertEquals(data, result);
    }

    @Test
    public void testToString() {
        final Buffer buf = bufferOf(ALPHABET);
        assertTrue(
                buf.toString().contains(
                        Integer.toString(ALPHABET_BYTES.length)));
    }
}
