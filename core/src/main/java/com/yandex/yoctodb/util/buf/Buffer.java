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

import com.yandex.yoctodb.util.UnsignedByteArrays;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
            final FileChannel file) {
        return new FileChannelBuffer(file);
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

    public abstract Buffer get(byte[] dst, int offset, int length);

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

        return compareTo(that) == 0;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("[pos=");
        sb.append(position());
        sb.append(" lim=");
        sb.append(limit());
        sb.append(" rem=");
        sb.append(remaining());
        sb.append("]");
        return sb.toString();
    }
}
