/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.buf;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Synchronized big-endian implementation of {@link Buffer} based on
 * {@link RandomAccessFile}
 *
 * @author incubos
 */
@NotThreadSafe
public final class SynchronizedFileBuffer extends Buffer {
    @NotNull
    private final FileWrapper file;
    private long position = 0L;
    private final long offset;
    private final long limit;

    public SynchronizedFileBuffer(
            @NotNull
            final RandomAccessFile file) {
        this.file = new FileWrapper(file);
        this.offset = 0L;
        try {
            this.limit = file.length();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SynchronizedFileBuffer(
            @NotNull
            final FileWrapper file,
            final long offset,
            final long limit) {
        this.file = file;
        this.offset = offset;
        this.limit = limit;
    }

    private static final class FileWrapper {
        private final Object lock = new Object();

        @GuardedBy("lock")
        private long position = -1L;

        @GuardedBy("lock")
        @NotNull
        private final RandomAccessFile file;

        private FileWrapper(
                @NotNull
                final RandomAccessFile file) {
            this.file = file;
        }

        public void get(
                final long position,
                final byte[] dst) {
            try {
                synchronized (lock) {
                    if (this.position != position) {
                        file.seek(position);
                    }

                    int offset = 0;
                    while (offset != dst.length) {
                        final int len =
                                file.read(
                                        dst,
                                        offset,
                                        dst.length - offset);
                        if (len == -1) {
                            throw new IllegalStateException(
                                    "End of file reached");
                        }
                        offset += len;
                    }
                    this.position += dst.length;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public byte get(final long position) {
            try {
                synchronized (lock) {
                    if (this.position != position) {
                        file.seek(position);
                    }

                    final int result = (byte) file.read();
                    if (result == -1) {
                        throw new IllegalStateException(
                                "End of file reached");
                    }
                    this.position++;
                    return (byte) result;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public int getInt(final long position) {
            try {
                synchronized (lock) {
                    if (this.position != position) {
                        file.seek(position);
                    }

                    final int result = file.readInt();
                    this.position += Ints.BYTES;
                    return result;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public long getLong(final long position) {
            try {
                synchronized (lock) {
                    if (this.position != position) {
                        file.seek(position);
                    }

                    final long result = file.readLong();
                    this.position += Longs.BYTES;
                    return result;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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
    public Buffer get(final byte[] dst) {
        assert dst.length <= remaining();

        file.get(this.offset + this.position, dst);
        this.position += dst.length;
        return this;
    }

    @Override
    public byte get() {
        final byte result = file.get(this.offset + this.position);
        this.position++;
        return result;
    }

    @Override
    public byte get(final long index) {
        return file.get(this.offset + index);
    }

    @Override
    public int getInt() {
        final int result = file.getInt(this.offset + this.position);
        this.position += Ints.BYTES;
        return result;
    }

    @Override
    public int getInt(final long index) {
        return file.getInt(this.offset + index);
    }

    @Override
    public long getLong() {
        final long result = file.getLong(this.offset + this.position);
        this.position += Longs.BYTES;
        return result;
    }

    @Override
    public long getLong(final long index) {
        return file.getLong(this.offset + index);
    }

    @Override
    public Buffer slice(final long from, final long size) {
        assert size >= 0;
        assert 0 <= from && from <= limit;
        assert from + size <= limit;

        return new SynchronizedFileBuffer(
                file,
                offset + from,
                size);
    }
}
