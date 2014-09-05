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
import com.yandex.yoctodb.util.UnsignedByteArrays;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Read only {@code long} addressed implementation of {@link java.nio.Buffer}
 *
 * @author incubos
 */
@NotThreadSafe
public abstract class Buffer implements Comparable<Buffer> {
    @NotNull
    public static Buffer from(
            @NotNull
            final ByteBuffer buf) {
        return new ByteBufferWrapper(buf);
    }

    @NotNull
    public static Buffer from(
            @NotNull
            final byte[] buf) {
        return Buffer.from(ByteBuffer.wrap(buf));
    }

    @NotNull
    public static Buffer from(
            @NotNull
            final RandomAccessFile file) {
        return new SynchronizedFileBuffer(file);
    }

    public abstract long position();

    public abstract Buffer position(final long position);

    public abstract Buffer advance(final long bytes);

    public abstract long limit();

    public abstract boolean hasRemaining();

    public abstract long remaining();

    public byte[] toByteArray() {
        assert remaining() <= Integer.MAX_VALUE;

        final byte[] result = new byte[(int) remaining()];
        get(result);
        return result;
    }

    public abstract Buffer get(byte[] dst);

    public abstract byte get();

    public abstract byte get(long index);

    public abstract int getInt();

    public abstract int getInt(long index);

    public abstract long getLong();

    public abstract long getLong(long index);

    public Buffer slice() {
        return slice(remaining());
    }

    public Buffer slice(long size) {
        assert size <= remaining();
        return slice(position(), size);
    }

    public abstract Buffer slice(long from, long size);

    @Override
    public boolean equals(final Object ob) {
        if (this == ob) {
            return true;
        }

        if (!(ob instanceof Buffer)) {
            return false;
        }

        final Buffer that = (Buffer) ob;
        long length = this.remaining();
        if (length != that.remaining()) {
            return false;
        }

        // Adapted from Guava UnsignedBytes

        long left = this.position();
        long right = that.position();
        for (;
             length >= Longs.BYTES;
             left += Longs.BYTES, right += Longs.BYTES, length -= Longs.BYTES)
            if (this.getLong(left) != that.getLong(right)) {
                return false;
            }

        if (length >= Ints.BYTES) {
            if (this.getInt(left) != that.getInt(right)) {
                return false;
            }
            left += Ints.BYTES;
            right += Ints.BYTES;
            length -= Ints.BYTES;
        }

        for (; length > 0; left++, right++, length--)
            if (this.get(left) != that.get(right)) {
                return false;
            }

        return true;
    }

    @Override
    public int hashCode() {
        long h = 1;
        final long l = limit();
        for (long i = position(); i < l; i++)
            h = 31 * h + get(i);
        return (int) h;
    }

    @Override
    public int compareTo(
            @NotNull
            final Buffer that) {
        return UnsignedByteArrays.compare(this, that);
    }
}
