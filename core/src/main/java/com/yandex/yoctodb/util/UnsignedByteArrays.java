/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util;

import com.google.common.base.Charsets;
import com.google.common.primitives.*;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
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

    private static final Charset UTF8 = Charsets.UTF_8;

    @NotNull
    public static UnsignedByteArray raw(
            @NotNull
            final byte[] bytes) {
        return new UnsignedByteArray(bytes);
    }

    @NotNull
    public static UnsignedByteArray from(
            @NotNull
            final String s) {
        return raw(s.getBytes(UTF8));
    }

    @NotNull
    public static UnsignedByteArray from(final long l) {
        return raw(Longs.toByteArray(l ^ Long.MIN_VALUE));
    }

    @NotNull
    public static UnsignedByteArray from(final int i) {
        return raw(Ints.toByteArray(i ^ Integer.MIN_VALUE));
    }

    @NotNull
    public static UnsignedByteArray from(final short s) {
        return raw(Shorts.toByteArray((short) (s ^ Short.MIN_VALUE)));
    }

    @NotNull
    public static UnsignedByteArray from(final byte b) {
        return raw(new byte[]{(byte) (b ^ Byte.MIN_VALUE)});
    }

    public final static byte BOOLEAN_TRUE_IN_BYTE = 0x1;
    public final static byte BOOLEAN_FALSE_IN_BYTE = 0x0;
    private final static UnsignedByteArray TRUE = raw(new byte[]{0x1});
    private final static UnsignedByteArray FALSE = raw(new byte[]{0x0});

    @NotNull
    public static UnsignedByteArray from(final boolean b) {
        return b ? TRUE : FALSE;
    }

    public static int compare(
            @NotNull
            final ByteBuffer left,
            int leftFrom,
            final int leftLength,
            @NotNull
            final ByteBuffer right,
            int rightFrom,
            final int rightLength) {
        assert leftLength > 0;
        assert rightLength > 0;

        // Adapted from Guava UnsignedBytes

        int length = Math.min(leftLength, rightLength);

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

        return leftLength - rightLength;
    }

    public static int compare(
            @NotNull
            final ByteBuffer left,
            int leftFrom,
            final int leftLength,
            @NotNull
            final ByteBuffer right) {
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
            final ByteBuffer left,
            @NotNull
            final ByteBuffer right) {
        return compare(
                left,
                left.position(),
                left.remaining(),
                right,
                right.position(),
                right.remaining());
    }
}

