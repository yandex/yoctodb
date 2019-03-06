/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util;

import com.google.common.base.Charsets;
import com.google.common.primitives.*;
import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;

/**
 * A utility class containing methods to transform primitive Java types to big
 * endian unsigned comparable byte arrays
 *
 * @author incubos
 */
public class UnsignedByteArrays {
    private UnsignedByteArrays() {
        //
    }

    // For test coverage
    static {
        new UnsignedByteArrays();
    }

    private static final Charset UTF8 = Charsets.UTF_8;

    @NotNull
    public static UnsignedByteArray from(
            @NotNull
            final byte[] bytes) {
        return new UnsignedByteArray(bytes);
    }

    @NotNull
    public static UnsignedByteArray from(
            @NotNull
            final Buffer buffer) {
        return from(buffer.toByteArray());
    }

    @NotNull
    public static UnsignedByteArray from(
            @NotNull
            final String s) {
        return from(s.getBytes(UTF8));
    }

    @NotNull
    public static String toString(
            @NotNull
            final UnsignedByteArray bytes) {
        return new String(bytes.data, UTF8);
    }

    @NotNull
    public static UnsignedByteArray from(final long l) {
        return from(Longs.toByteArray(l ^ Long.MIN_VALUE));
    }

    public static long toLong(
            @NotNull
            final UnsignedByteArray bytes) {
        if (bytes.length() != Longs.BYTES)
            throw new IllegalArgumentException("Wrong length");

        return Longs.fromByteArray(bytes.data) ^ Long.MIN_VALUE;
    }

    @NotNull
    public static UnsignedByteArray from(final int i) {
        return from(Ints.toByteArray(i ^ Integer.MIN_VALUE));
    }

    @NotNull
    public static UnsignedByteArray from(final char c) {
        return from(Chars.toByteArray(c));
    }

    public static int toInt(
            @NotNull
            final UnsignedByteArray bytes) {
        if (bytes.length() != Ints.BYTES)
            throw new IllegalArgumentException("Wrong length");

        return Ints.fromByteArray(bytes.data) ^ Integer.MIN_VALUE;
    }

    @NotNull
    public static UnsignedByteArray from(final short s) {
        return from(Shorts.toByteArray((short) (s ^ Short.MIN_VALUE)));
    }

    public static short toShort(
            @NotNull
            final UnsignedByteArray bytes) {
        if (bytes.length() != Shorts.BYTES)
            throw new IllegalArgumentException("Wrong length");

        return (short) (Shorts.fromByteArray(bytes.data) ^ Short.MIN_VALUE);
    }

    @NotNull
    public static UnsignedByteArray from(final byte b) {
        return from(new byte[]{(byte) (b ^ Byte.MIN_VALUE)});
    }

    public static byte toByte(
            @NotNull
            final UnsignedByteArray bytes) {
        if (bytes.length() != 1)
            throw new IllegalArgumentException("Wrong length");

        return (byte) (bytes.data[0] ^ Byte.MIN_VALUE);
    }

    private final static UnsignedByteArray TRUE = from(new byte[]{0x1});
    private final static UnsignedByteArray FALSE = from(new byte[]{0x0});

    @NotNull
    public static UnsignedByteArray from(final boolean b) {
        return b ? TRUE : FALSE;
    }

    public static boolean toBoolean(
            @NotNull
            final UnsignedByteArray bytes) {
        if (bytes.equals(TRUE))
            return true;
        else if (bytes.equals(FALSE))
            return false;
        else
            throw new IllegalArgumentException("Unexpected value");
    }

    private static int compare(
            @NotNull
            final Buffer left,
            long leftFrom,
            final long leftLength,
            @NotNull
            final Buffer right,
            long rightFrom,
            final long rightLength) {
        if (leftLength == 0 || rightLength == 0) { // one of arrays is empty
            return Long.compare(leftLength, rightLength);
        }

        // Adapted from Guava UnsignedBytes

        long length = Math.min(leftLength, rightLength);

        for (;
             length >= Longs.BYTES;
             leftFrom += Longs.BYTES,
                     rightFrom += Longs.BYTES,
                     length -= Longs.BYTES) {
            final long lw = left.getLong(leftFrom);
            final long rw = right.getLong(rightFrom);
            if (lw != rw) {
                return UnsignedLongs.compare(lw, rw);
            }
        }

        if (length >= Ints.BYTES) {
            final int lw = left.getInt(leftFrom);
            final int rw = right.getInt(rightFrom);
            if (lw != rw) {
                return UnsignedInts.compare(lw, rw);
            }
            leftFrom += Ints.BYTES;
            rightFrom += Ints.BYTES;
            length -= Ints.BYTES;
        }

        for (; length > 0; leftFrom++, rightFrom++, length--) {
            final int result =
                    UnsignedBytes.compare(
                            left.get(leftFrom),
                            right.get(rightFrom));

            if (result != 0) {
                return result;
            }
        }

        return Long.compare(leftLength, rightLength);
    }

    public static int compare(
            @NotNull
            final Buffer left,
            final long leftFrom,
            final long leftLength,
            @NotNull
            final Buffer right) {
        return compare(
                left,
                leftFrom,
                leftLength,
                right,
                right.position(),
                right.remaining());
    }

    public static int compare(
            @NotNull
            final Buffer left,
            @NotNull
            final Buffer right) {
        return compare(
                left,
                left.position(),
                left.remaining(),
                right,
                right.position(),
                right.remaining());
    }
}
