/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A wrapper for {@code byte[]} providing {@code equals()} and {@code
 * hashCode()} implementations. The array is supposed to be immutable.
 *
 * Bytes in the byte array are treated as unsigned.
 *
 * @author incubos
 */
@Immutable
@NotThreadSafe
public final class UnsignedByteArray
        implements Comparable<UnsignedByteArray>,
                   Iterable<Byte>,
                   OutputStreamWritable {
    @NotNull
    final byte[] data;
    private int hash;

    UnsignedByteArray(
            @NotNull
            final byte[] data) {
        this.data = data;
    }

    @NotNull
    public Buffer toByteBuffer() {
        return Buffer.from(ByteBuffer.wrap(data));
    }

    @Override
    public Iterator<Byte> iterator() {
        return Bytes.asList(data).iterator();
    }

    public int length() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }

    @Override
    public long getSizeInBytes() {
        return length();
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        os.write(data);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = Arrays.hashCode(data);
        }
        return hash;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final UnsignedByteArray that = (UnsignedByteArray) o;

        return Arrays.equals(data, that.data);
    }

    @Override
    public int compareTo(
            @NotNull
            final UnsignedByteArray o) {
        if (this == o) {
            return 0;
        } else {
            return UnsignedBytes.lexicographicalComparator()
                                .compare(this.data, o.data);
        }
    }
}
