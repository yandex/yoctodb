/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.v1.immutable.segment;

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
