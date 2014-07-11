/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.util;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static com.yandex.yoctodb.util.UnsignedByteArrays.*;

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

        assertTrue(raw(a01).compareTo(raw(a10)) < 0);
        assertTrue(raw(a01).compareTo(raw(a01)) == 0);
        assertTrue(raw(a10).compareTo(raw(a10)) == 0);
        assertTrue(raw(a10).compareTo(raw(a01)) > 0);
    }

    @Test
    public void testString() {
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

    @Test
    public void testBoolean() {
        assertTrue(from(true).compareTo(from(true)) == 0);
        assertTrue(from(false).compareTo(from(false)) == 0);

        assertTrue(from(false).compareTo(from(true)) < 0);

        assertTrue(from(true).compareTo(from(false)) > 0);
    }

    @Test
    public void testByte() {
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

    @Test
    public void testShort() {
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

    @Test
    public void testInt() {
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

    @Test
    public void testLong() {
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
}
