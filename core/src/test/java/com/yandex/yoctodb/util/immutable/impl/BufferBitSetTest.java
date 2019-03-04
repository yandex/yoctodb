/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class BufferBitSetTest {
    private static final Buffer bitsetBuffer;
    private static final Buffer emptyBuffer;
    private static final Buffer oneBuffer;

    static {
        ByteBuffer b = ByteBuffer.allocate(48);
        b.putLong(0x5555555555555555L);
        b.putLong(0xAAAAAAAAAAAAAAAAL);
        b.putLong(0xDEADBEEFDEADBEEFL);
        b.putLong(0L);
        b.putLong(0xDEADBEEFDEADBEEFL);
        b.rewind();

        bitsetBuffer = Buffer.from(b);

        emptyBuffer = Buffer.from(ByteBuffer.allocate(8));

        ByteBuffer one = ByteBuffer.allocate(8);
        one.putLong(-1L);
        one.rewind();

        oneBuffer = Buffer.from(one);
    }

    @Test
    public void get() {
        assertTrue(BufferBitSet.get(bitsetBuffer, 0, 0));
        assertTrue(BufferBitSet.get(bitsetBuffer, 0, 2));
        assertTrue(BufferBitSet.get(bitsetBuffer, 0, 65));
        assertTrue(BufferBitSet.get(bitsetBuffer, 0, 127));

        assertFalse(BufferBitSet.get(bitsetBuffer, 0, 1));
        assertFalse(BufferBitSet.get(bitsetBuffer, 0, 63));
        assertFalse(BufferBitSet.get(bitsetBuffer, 0, 64));
        assertFalse(BufferBitSet.get(bitsetBuffer, 0, 126));
    }

    @Test
    public void nextSetBit() {
        assertEquals(0, BufferBitSet.nextSetBit(bitsetBuffer, 0, 128, 0));
        assertEquals(2, BufferBitSet.nextSetBit(bitsetBuffer, 0, 128, 1));
        assertEquals(65, BufferBitSet.nextSetBit(bitsetBuffer, 0, 128, 64));
        assertEquals(127, BufferBitSet.nextSetBit(bitsetBuffer, 0, 128, 126));
        assertEquals(160, BufferBitSet.nextSetBit(bitsetBuffer, 0, 192, 160));
        assertEquals(256, BufferBitSet.nextSetBit(bitsetBuffer, 0, 320, 200));
        assertEquals(-1, BufferBitSet.nextSetBit(bitsetBuffer, 0, 192, 193));
    }

    @Test
    public void nextSetBitOne() {
        assertEquals(0, BufferBitSet.nextSetBit(oneBuffer, 0, 128, 0));
    }

    @Test(expected = AssertionError.class)
    public void nextSetBitError() {
        assertEquals(-1, BufferBitSet.nextSetBit(bitsetBuffer, 0, 128, 512));
    }

    @Test
    public void nextSetBitNotFound() {
        assertEquals(-1, BufferBitSet.nextSetBit(emptyBuffer, 0, 64, 0));
    }

    @Test
    public void cardinalityTo() {
        assertEquals(0, BufferBitSet.cardinalityTo(bitsetBuffer, 0, 0));
        assertEquals(1, BufferBitSet.cardinalityTo(bitsetBuffer, 0, 1));
        assertEquals(32, BufferBitSet.cardinalityTo(bitsetBuffer, 0, 63));
        assertEquals(31, BufferBitSet.cardinalityTo(bitsetBuffer, 0, 62));
        assertEquals(33, BufferBitSet.cardinalityTo(bitsetBuffer, 0, 66));
    }

    @Test
    public void arraySize() {
    }
}
