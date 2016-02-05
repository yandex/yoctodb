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

import com.google.common.io.ByteStreams;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import org.junit.Test;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link MessageDigestOutputStreamWrapper}
 *
 * @author incubos
 */
public class MessageDigestOutputStreamWrapperTest {
    private static final MessageDigest md;

    static {
        try {
            md = MessageDigest.getInstance(
                    V1DatabaseFormat.getMessageDigestAlgorithm());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void byteDigest() throws IOException {
        final MessageDigestOutputStreamWrapper w1 =
                new MessageDigestOutputStreamWrapper(
                        ByteStreams.nullOutputStream(),
                        md);
        final byte v1 = 42;
        w1.write(v1);
        w1.flush();
        w1.close();
        final byte[] d1 = w1.digest();

        final MessageDigestOutputStreamWrapper w2 =
                new MessageDigestOutputStreamWrapper(
                        ByteStreams.nullOutputStream(),
                        md);
        final byte v2 = 43;
        w2.write(v2);
        w2.flush();
        w2.close();
        final byte[] d2 = w2.digest();

        assertTrue(!Arrays.equals(d1, d2));
    }

    @Test
    public void bufDigest() throws IOException {
        final MessageDigestOutputStreamWrapper w1 =
                new MessageDigestOutputStreamWrapper(
                        ByteStreams.nullOutputStream(),
                        md);
        final byte[] v1 = "v1".getBytes();
        w1.write(v1);
        w1.flush();
        w1.close();
        final byte[] d1 = w1.digest();

        final MessageDigestOutputStreamWrapper w2 =
                new MessageDigestOutputStreamWrapper(
                        ByteStreams.nullOutputStream(),
                        md);
        final byte[] v2 = "v2".getBytes();
        w2.write(v2);
        w2.flush();
        w2.close();
        final byte[] d2 = w2.digest();

        assertTrue(!Arrays.equals(d1, d2));
    }
}
