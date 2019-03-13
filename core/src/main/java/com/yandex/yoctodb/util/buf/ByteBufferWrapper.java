/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.buf;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * {@link ByteBuffer} wrapper implementation of {@link Buffer}
 *
 * @author incubos
 */
@NotThreadSafe
public final class ByteBufferWrapper extends Buffer {
    @NotNull
    private final ByteBuffer delegate;

    public ByteBufferWrapper(
            @NotNull
            final ByteBuffer delegate) {
        this.delegate = delegate;
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public Buffer position(final long position) {
        assert position <= Integer.MAX_VALUE;

        delegate.position((int) position);

        return this;
    }

    @Override
    public Buffer advance(final long bytes) {
        assert delegate.position() + bytes <= Integer.MAX_VALUE;

        delegate.position((int) (delegate.position() + bytes));

        return this;
    }

    @Override
    public long limit() {
        return delegate.limit();
    }

    @Override
    public boolean hasRemaining() {
        return delegate.hasRemaining();
    }

    @Override
    public long remaining() {
        return delegate.remaining();
    }

    @Override
    public Buffer get(byte[] dst, int offset, int length) {
        delegate.get(dst, offset, length);
        return this;
    }

    @Override
    public Buffer get(final byte[] dst) {
        delegate.get(dst);
        return this;
    }

    @Override
    public byte get() {
        return delegate.get();
    }

    @Override
    public byte get(final long index) {
        assert index <= Integer.MAX_VALUE;

        return delegate.get((int) index);
    }

    @Override
    public int getInt() {
        return delegate.getInt();
    }

    @Override
    public int getInt(final long index) {
        assert index <= Integer.MAX_VALUE;

        return delegate.getInt((int) index);
    }

    @Override
    public long getLong() {
        return delegate.getLong();
    }

    @Override
    public long getLong(final long index) {
        assert index <= Integer.MAX_VALUE;

        return delegate.getLong((int) index);
    }

    @Override
    public char getChar() {
        return delegate.getChar();
    }

    @Override
    public char getChar(long index) {
        assert index <= Integer.MAX_VALUE;

        return delegate.getChar((int) index);
    }

    @Override
    public short getShort() {
        return delegate.getShort();
    }

    @Override
    public short getShort(final long index) {
        assert index <= Integer.MAX_VALUE;

        return delegate.getShort((int) index);
    }

    @Override
    public Buffer slice() {
        return Buffer.from(delegate.slice());
    }

    @Override
    public Buffer slice(final long length) {
        assert length <= delegate.remaining();

        final ByteBuffer slice = delegate.duplicate();
        slice.limit((int) (slice.position() + length));

        return Buffer.from(slice.slice());
    }

    @Override
    public Buffer slice(final long from, final long size) {
        assert size >= 0;
        assert 0 <= from && from <= delegate.limit();
        assert from + size <= delegate.limit();

        final ByteBuffer slice = delegate.duplicate();
        slice.position((int) from);
        slice.limit((int) (from + size));

        return Buffer.from(slice.slice());
    }
}
