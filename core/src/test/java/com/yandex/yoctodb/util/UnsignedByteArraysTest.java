/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.util.Iterator;

import static com.yandex.yoctodb.util.UnsignedByteArrays.*;
import static org.junit.Assert.*;

/**
 * Tests for {@link UnsignedByteArrays}
 *
 * @author incubos
 */
public class UnsignedByteArraysTest {
    @Test
    public void testByteArray() {
        final byte[] a01 = {0, 1};
        final byte[] a10 = {1, 0};

        assertTrue(from(a01).compareTo(from(a10)) < 0);
        assertTrue(from(a01).compareTo(from(a01)) == 0);
        assertTrue(from(a10).compareTo(from(a10)) == 0);
        assertTrue(from(a10).compareTo(from(a01)) > 0);
    }

    @Test
    public void testBuffer() {
        final byte[] data = {0, 1, 2, 3};
        assertEquals(from(data), from(Buffer.from(data)));
        assertEquals(from(data).toByteBuffer(), Buffer.from(data));
    }

    @Test
    public void testString() {
        // Converting
        assertEquals("", UnsignedByteArrays.toString(from("")));
        assertEquals("a", UnsignedByteArrays.toString(from("a")));
        assertEquals("test", UnsignedByteArrays.toString(from("test")));
        assertEquals("Б", UnsignedByteArrays.toString(from("Б")));
        assertEquals("Тест", UnsignedByteArrays.toString(from("Тест")));

        // ASCII
        assertTrue(from("").compareTo(from("aaa")) < 0);
        assertTrue(from("a").compareTo(from("b")) < 0);
        assertTrue(from("test").compareTo(from("test")) == 0);
        assertTrue(from("test").compareTo(from("test2")) < 0);

        // Unicode
        assertTrue(from("").compareTo(from("Ы")) < 0);
        assertTrue(from("Б").compareTo(from("Ы")) < 0);
        assertTrue(from("Тест").compareTo(from("Тест")) == 0);
        assertTrue(from("Тест").compareTo(from("Тест2")) < 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBooleanTooBig() {
        toBoolean(from(new byte[]{0x0, 0x1}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBooleanTooSmall() {
        toBoolean(from(new byte[]{}));
    }

    @Test
    public void testBoolean() {
        assertEquals(true, toBoolean(from(true)));
        assertEquals(false, toBoolean(from(false)));

        assertTrue(from(true).compareTo(from(true)) == 0);
        assertTrue(from(false).compareTo(from(false)) == 0);

        assertTrue(from(false).compareTo(from(true)) < 0);

        assertTrue(from(true).compareTo(from(false)) > 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testByteTooBig() {
        toByte(from(new byte[]{0x0, 0x1}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testByteTooSmall() {
        toByte(from(new byte[]{}));
    }

    @Test
    public void testByte() {
        assertEquals((byte) 0, toByte(from((byte) 0)));
        assertEquals(Byte.MIN_VALUE, toByte(from(Byte.MIN_VALUE)));
        assertEquals(Byte.MAX_VALUE, toByte(from(Byte.MAX_VALUE)));

        assertTrue(from((byte) 0).compareTo(from((byte) 0)) == 0);
        assertTrue(from(Byte.MIN_VALUE).compareTo(from(Byte.MIN_VALUE)) == 0);
        assertTrue(from(Byte.MAX_VALUE).compareTo(from(Byte.MAX_VALUE)) == 0);

        assertTrue(from((byte) 1).compareTo(from((byte) 2)) < 0);
        assertTrue(from((byte) -2).compareTo(from((byte) -1)) < 0);
        assertTrue(from((byte) -1).compareTo(from((byte) 1)) < 0);
        assertTrue(from(Byte.MIN_VALUE).compareTo(from(Byte.MAX_VALUE)) < 0);
        assertTrue(from(Byte.MIN_VALUE).compareTo(from((byte) 0)) < 0);
        assertTrue(from((byte) 0).compareTo(from(Byte.MAX_VALUE)) < 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShortTooBig() {
        toShort(from(new byte[]{0x0, 0x1, 0x2}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShortTooSmall() {
        toShort(from(new byte[]{0x0}));
    }

    @Test
    public void testShort() {
        assertEquals((short) 0, toShort(from((short) 0)));
        assertEquals(Short.MIN_VALUE, toShort(from(Short.MIN_VALUE)));
        assertEquals(Short.MAX_VALUE, toShort(from(Short.MAX_VALUE)));

        assertTrue(from((short) 0).compareTo(from((short) 0)) == 0);
        assertTrue(from(Short.MIN_VALUE).compareTo(from(Short.MIN_VALUE)) == 0);
        assertTrue(from(Short.MAX_VALUE).compareTo(from(Short.MAX_VALUE)) == 0);

        assertTrue(from((short) 1).compareTo(from((short) 2)) < 0);
        assertTrue(from((short) -2).compareTo(from((short) -1)) < 0);
        assertTrue(from((short) -1).compareTo(from((short) 1)) < 0);
        assertTrue(from(Short.MIN_VALUE).compareTo(from(Short.MAX_VALUE)) < 0);
        assertTrue(from(Short.MIN_VALUE).compareTo(from((short) 0)) < 0);
        assertTrue(from((short) 0).compareTo(from(Short.MAX_VALUE)) < 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIntTooBig() {
        toInt(from(new byte[]{0x0, 0x1, 0x2, 0x3, 0x4}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIntTooSmall() {
        toInt(from(new byte[]{0x0, 0x1, 0x2}));
    }

    @Test
    public void testInt() {
        assertEquals(0, toInt(from(0)));
        assertEquals(Integer.MIN_VALUE, toInt(from(Integer.MIN_VALUE)));
        assertEquals(Integer.MAX_VALUE, toInt(from(Integer.MAX_VALUE)));

        assertTrue(from(0).compareTo(from(0)) == 0);
        assertTrue(
                from(Integer.MIN_VALUE).compareTo(
                        from(Integer.MIN_VALUE)) == 0);
        assertTrue(
                from(Integer.MAX_VALUE).compareTo(
                        from(Integer.MAX_VALUE)) == 0);

        assertTrue(from(1).compareTo(from(2)) < 0);
        assertTrue(from(-2).compareTo(from(-1)) < 0);
        assertTrue(from(-1).compareTo(from(1)) < 0);
        assertTrue(
                from(Integer.MIN_VALUE).compareTo(
                        from(Integer.MAX_VALUE)) < 0);
        assertTrue(from(Integer.MIN_VALUE).compareTo(from(0)) < 0);
        assertTrue(from(0).compareTo(from(Integer.MAX_VALUE)) < 0);
        assertTrue(from(127).compareTo(from(128)) < 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLongTooBig() {
        toLong(from(new byte[]{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8}));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLongTooSmall() {
        toLong(from(new byte[]{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6}));
    }

    @Test
    public void testLong() {
        assertEquals(0L, toLong(from(0L)));
        assertEquals(Long.MIN_VALUE, toLong(from(Long.MIN_VALUE)));
        assertEquals(Long.MAX_VALUE, toLong(from(Long.MAX_VALUE)));

        assertTrue(from(0L).compareTo(from(0L)) == 0);
        assertTrue(from(Long.MIN_VALUE).compareTo(from(Long.MIN_VALUE)) == 0);
        assertTrue(from(Long.MAX_VALUE).compareTo(from(Long.MAX_VALUE)) == 0);

        assertTrue(from(1L).compareTo(from(2L)) < 0);
        assertTrue(from(-2L).compareTo(from(-1L)) < 0);
        assertTrue(from(-1L).compareTo(from(1L)) < 0);
        assertTrue(from(Long.MIN_VALUE).compareTo(from(Long.MAX_VALUE)) < 0);
        assertTrue(from(Long.MIN_VALUE).compareTo(from(0L)) < 0);
        assertTrue(from(0L).compareTo(from(Long.MAX_VALUE)) < 0);
    }

    @Test
    public void iterable() {
        final Iterator<Byte> iter = from(new byte[]{1, 2, 3}).iterator();
        assertEquals(1, iter.next().byteValue());
        assertEquals(2, iter.next().byteValue());
        assertEquals(3, iter.next().byteValue());
        assertFalse(iter.hasNext());
    }
}
