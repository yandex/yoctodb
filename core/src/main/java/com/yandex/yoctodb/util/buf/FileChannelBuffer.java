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

    public FileChannelBuffer(
            @NotNull
            final FileChannel ch) {
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

        final ByteBuffer byteBuf = byteBufCache.get();
        try {
            final int c = ch.read(byteBuf, this.offset + this.position);
            assert c == Byte.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position++;

        return byteBuf.get(0);
    }

    @Override
    public byte get(final long index) {
        assert 0 <= index && index < limit;

        final ByteBuffer byteBuf = byteBufCache.get();
        try {
            final int c = ch.read(byteBuf, this.offset + index);
            assert c == Byte.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return byteBuf.get(0);
    }

    @Override
    public int getInt() {
        assert remaining() >= Integer.BYTES;;

        final ByteBuffer intBuf = intBufCache.get();
        intBuf.rewind();
        try {
            final int c = ch.read(intBuf, this.offset + this.position);
            assert c == Integer.BYTES;;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position += Integer.BYTES;

        return intBuf.getInt(0);
    }

    @Override
    public int getInt(final long index) {
        assert index + Integer.BYTES <= limit;

        final ByteBuffer intBuf = intBufCache.get();
        try {
            final int c = ch.read(intBuf, this.offset + index);
            assert c == Integer.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return intBuf.getInt(0);
    }

    @Override
    public long getLong() {
        assert remaining() >= Long.BYTES;

        final ByteBuffer longBuf = longBufCache.get();
        try {
            final int c = ch.read(longBuf, this.offset + this.position);
            assert c == Long.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position += Long.BYTES;

        return longBuf.getLong(0);
    }

    @Override
    public long getLong(final long index) {
        assert index + 8 <= limit;

        final ByteBuffer longBuf = longBufCache.get();
        try {
            final int c = ch.read(longBuf, this.offset + index);
            assert c == Long.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return longBuf.getLong(0);
    }

    @Override
    public char getChar() {
        assert remaining() >= Character.BYTES;

        final ByteBuffer charBuf = charBufCache.get();
        try {
            final int c = ch.read(charBuf, this.offset + this.position);
            assert c == Character.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position += Character.BYTES;

        return charBuf.getChar(0);
    }

    @Override
    public char getChar(final long index) {
        assert index + Character.BYTES <= limit;

        final ByteBuffer charBuf = charBufCache.get();
        try {
            final int c = ch.read(charBuf, this.offset + index);
            assert c == Character.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return charBuf.getChar(0);
    }

    @Override
    public short getShort() {
        assert remaining() >= Short.BYTES;

        final ByteBuffer shortBuf = shortBufCache.get();
        try {
            final int c = ch.read(shortBuf, this.offset + this.position);
            assert c == Short.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.position += Short.BYTES;

        return shortBuf.getShort(0);
    }

    @Override
    public short getShort(final long index) {
        assert index + Short.BYTES <= limit;

        final ByteBuffer shortBuf = shortBufCache.get();
        try {
            final int c = ch.read(shortBuf, this.offset + index);
            assert c == Short.BYTES;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return shortBuf.getShort(0);
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

    // Temporary buffers
    private final ThreadLocal<ByteBuffer> byteBufCache =
            new ThreadLocal<ByteBuffer>() {
                @Override
                protected ByteBuffer initialValue() {
                    return ByteBuffer.allocate(1);
                }

                @Override
                public ByteBuffer get() {
                    final ByteBuffer result = super.get();
                    result.rewind();
                    return result;
                }
            };

    private final ThreadLocal<ByteBuffer> shortBufCache =
            new ThreadLocal<ByteBuffer>() {
                @Override
                protected ByteBuffer initialValue() {
                    return ByteBuffer.allocate(Short.BYTES);
                }

                @Override
                public ByteBuffer get() {
                    final ByteBuffer result = super.get();
                    result.rewind();
                    return result;
                }
            };

    private final ThreadLocal<ByteBuffer> intBufCache =
            new ThreadLocal<ByteBuffer>() {
                @Override
                protected ByteBuffer initialValue() {
                    return ByteBuffer.allocate(4);
                }

                @Override
                public ByteBuffer get() {
                    final ByteBuffer result = super.get();
                    result.rewind();
                    return result;
                }
            };

    private final ThreadLocal<ByteBuffer> longBufCache =
            new ThreadLocal<ByteBuffer>() {
                @Override
                protected ByteBuffer initialValue() {
                    return ByteBuffer.allocate(8);
                }

                @Override
                public ByteBuffer get() {
                    final ByteBuffer result = super.get();
                    result.rewind();
                    return result;
                }
            };

    private final ThreadLocal<ByteBuffer> charBufCache =
            new ThreadLocal<ByteBuffer>() {
                @Override
                protected ByteBuffer initialValue() {
                    return ByteBuffer.allocate(Character.BYTES);
                }

                @Override
                public ByteBuffer get() {
                    final ByteBuffer result = super.get();
                    result.rewind();
                    return result;
                }
            };
}
