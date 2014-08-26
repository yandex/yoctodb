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

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;

/**
 * Wraps {@link OutputStream} and updates {@link MessageDigest} accordingly
 *
 * @author incubos
 */
public final class MessageDigestOutputStreamWrapper extends OutputStream {
    @NotNull
    private final OutputStream delegate;
    @NotNull
    private final MessageDigest digest;

    public MessageDigestOutputStreamWrapper(
            @NotNull
            final OutputStream delegate,
            @NotNull
            final MessageDigest digest) {
        this.delegate = delegate;
        this.digest = digest;
    }

    @NotNull
    public byte[] digest() {
        return digest.digest();
    }

    @Override
    public void write(final int b) throws IOException {
        digest.update((byte) b);
        delegate.write(b);
    }

    @Override
    public void write(final byte[] b) throws IOException {
        digest.update(b);
        delegate.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len)
            throws IOException {
        digest.update(b, off, len);
        delegate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
