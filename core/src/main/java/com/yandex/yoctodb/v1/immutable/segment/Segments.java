/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

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
     * Reads {@code long} segment size, extracts the slice and advances
     * {@code from} buffer
     *
     * @param from buffer to extract segment slice from
     * @return segment slice
     */
    @NotNull
    public static Buffer extract(
            @NotNull
            final Buffer from) {
        final long size = from.getLong();
        final Buffer result = from.slice(size);
        from.advance(size);
        return result;
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
            final Buffer from) {
        final int size = from.getInt();
        final byte[] buffer = new byte[size];
        from.get(buffer);

        return new String(buffer);
    }

    public static Buffer calculateDigest(
            @NotNull
            final Buffer buffer,
            @NotNull
            final String messageDigestAlgorithm) {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance(messageDigestAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.reset();

        // get byte buffer without digest length and without digest content
        final Buffer data =
                buffer.slice(buffer.remaining() - md.getDigestLength() - 8);
        final byte[] buf =
                new byte[Math.min((int) Math.max(buffer.remaining(), 8192L), 8192)];
        while (data.remaining() >= buf.length) {
            data.get(buf);
            md.update(buf);
        }
        if (data.hasRemaining()) {
            md.update(data.toByteArray());
        }

        return Buffer.from(md.digest());
    }
}
