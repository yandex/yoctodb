/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.buf;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Big-endian implementation of {@link Buffer} based on {@link FileChannel}
 *
 * @author incubos
 */
@NotThreadSafe
public final class FileChannelBuffer extends Buffer {
    @NotNull
    private final FileChannel ch;
    private final long offset;
    private final long limit;
    private long position;

    // Temporary buffers
    private final ByteBuffer byteBuf = ByteBuffer.allocate(1);
    private final ByteBuffer intBuf = ByteBuffer.allocate(4);
    private final ByteBuffer longBuf = ByteBuffer.allocate(8);

    public FileChannelBuffer(
            @NotNull
            final FileChannel ch) {
        assert ch.isOpen();

        this.ch = ch;
        this.offset = 0L;
        try {
            this.limit = ch.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.position = 0L;
    }

    private FileChannelBuffer(
            @NotNull
            final FileChannel ch,
            final long offset,
            final long limit) {
        assert ch.isOpen();
        assert 0 <= offset;
        assert 0 <= limit;
        try {
            assert offset + limit <= ch.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.ch = ch;
        this.offset = offset;
        this.limit = limit;
        this.position = 0L;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public Buffer position(final long position) {
        assert 0 <= position && position <= limit;

        this.position = position;

        return this;
    }

    @Override
    public Buffer advance(final long bytes) {
        assert position + bytes <= limit;

        this.position += bytes;

        return this;
    }

    @Override
    public long limit() {
        return limit;
    }

    @Override
    public boolean hasRemaining() {
        return position < limit;
    }

    @Override
    public long remaining() {
        return limit - position;
    }

    @Override
    public Buffer get(byte[] dst, int offset, int length) {
        assert length <= remaining();

        try {
            final int c =
                    ch.read(ByteBuffer.wrap(dst, offset, length), this.offset + this.position);
            assert c == length;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position += length;

        return this;
    }

    @Override
    public Buffer get(final byte[] dst) {
        assert dst.length <= remaining();

        try {
            final int c =
                    ch.read(ByteBuffer.wrap(dst), this.offset + this.position);
            assert c == dst.length;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position += dst.length;

        return this;
    }

    @Override
    public byte get() {
        assert hasRemaining();

        byteBuf.rewind();
        try {
            final int c = ch.read(byteBuf, this.offset + this.position);
            assert c == 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position++;

        return byteBuf.get(0);
    }

    @Override
    public byte get(final long index) {
        assert 0 <= index && index < limit;

        byteBuf.rewind();
        try {
            final int c = ch.read(byteBuf, this.offset + index);
            assert c == 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return byteBuf.get(0);
    }

    @Override
    public int getInt() {
        assert remaining() >= 4;

        intBuf.rewind();
        try {
            final int c = ch.read(intBuf, this.offset + this.position);
            assert c == 4;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position += 4;

        return intBuf.getInt(0);
    }

    @Override
    public int getInt(final long index) {
        assert index + 4 <= limit;

        intBuf.rewind();
        try {
            final int c = ch.read(intBuf, this.offset + index);
            assert c == 4;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return intBuf.getInt(0);
    }

    @Override
    public long getLong() {
        assert remaining() >= 8;

        longBuf.rewind();
        try {
            final int c = ch.read(longBuf, this.offset + this.position);
            assert c == 8;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position += 8;

        return longBuf.getLong(0);
    }

    @Override
    public long getLong(final long index) {
        assert index + 8 <= limit;

        longBuf.rewind();
        try {
            final int c = ch.read(longBuf, this.offset + index);
            assert c == 8;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return longBuf.getLong(0);
    }

    @Override
    public Buffer slice(final long from, final long size) {
        assert 0 <= from;
        assert 0 <= size;
        assert from + size <= limit;

        return new FileChannelBuffer(
                ch,
                this.offset + from,
                size);
    }
}
