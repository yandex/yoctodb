/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.v1.mutable.segment;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author svyatoslav
 */
public class Utils {
    public static byte[] calculateDigest(
            @NotNull
            final ByteBuffer buffer,
            @NotNull
            final String messageDigestAlgorithm) {
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance(messageDigestAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.reset();
        //get byte buffer without digest length and without digest content
        final ByteBuffer byteBuffer = buffer.duplicate();
        byteBuffer.limit(byteBuffer.limit() - md.getDigestLength() - 4);

        md.update(byteBuffer.slice());
        return md.digest();
    }
}
