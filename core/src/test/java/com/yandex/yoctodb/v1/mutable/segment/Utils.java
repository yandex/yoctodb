/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable.segment;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author svyatoslav
 */
public class Utils {
    public static byte[] calculateDigest(
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
        //get byte buffer without digest length and without digest content
        md.update(
                buffer.slice(
                        buffer.remaining() - md.getDigestLength() - 8)
                      .toByteArray());
        return md.digest();
    }
}
