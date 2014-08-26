/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Utility methods for segments
 *
 * @author incubos
 */
class Segments {
    private Segments() {
        //
    }

    /**
     * Reads {@code int} segment size, extracts the slice and advances
     * {@code from} buffer
     *
     * @param from buffer to extract segment slice from
     * @return segment slice
     */
    @NotNull
    public static ByteBuffer extract(
            @NotNull
            final ByteBuffer from) {
        final int size = from.getInt();
        final ByteBuffer buffer = from.duplicate();
        buffer.limit(buffer.position() + size);
        from.position(from.position() + size);
        return buffer.slice();
    }

    /**
     * Reads {@code int} string size, extracts the string and advances
     * {@code from} buffer
     *
     * @param from buffer to extract the string from
     * @return extracted string
     */
    @NotNull
    public static String extractString(
            @NotNull
            final ByteBuffer from) {
        final int size = from.getInt();
        final byte[] buffer = new byte[size];
        from.get(buffer);

        return new String(buffer);
    }


    public static byte[] calculateDigest(@NotNull ByteBuffer buffer,
                                         @NotNull String messageDigestAlgorithm) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(messageDigestAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.reset();
        //get byte buffer without digest length and without digest content
        ByteBuffer byteBuffer = buffer.duplicate();
        byteBuffer.limit(byteBuffer.limit() - md.getDigestLength() - 4);

        md.update(byteBuffer.slice());
        return md.digest();
    }
}
