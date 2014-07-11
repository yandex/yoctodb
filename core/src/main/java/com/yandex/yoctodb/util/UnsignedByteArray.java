/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.util;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A wrapper for {@code byte[]} providing {@code equals()} and {@code
 * hashCode()} implementations. The array is supposed to be immutable.
 * <p/>
 * Bytes in the byte array are treated as unsigned.
 *
 * @author incubos
 */
@Immutable
public final class UnsignedByteArray
        implements Comparable<UnsignedByteArray>,
                   Iterable<Byte>,
                   OutputStreamWritable {
    @NotNull
    private final byte[] data;
    private int hash;

    UnsignedByteArray(
            @NotNull
            final byte[] data) {
        this.data = data;
    }

    @NotNull
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(data);
    }

    @Override
    public Iterator<Byte> iterator() {
        return Bytes.asList(data).iterator();
    }

    public int length() {
        return data.length;
    }

    @Override
    public int getSizeInBytes() {
        return length();
    }

    @Override
    public void writeTo(
            @NotNull
            final
            OutputStream os) throws IOException {
        os.write(data);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int result = 1;
            for (byte element : data)
                result = 31 * result + element;
            hash = result;
        }
        return hash;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final UnsignedByteArray that = (UnsignedByteArray) o;

        final int length = data.length;
        final byte[] thatData = that.data;
        if (length != thatData.length) {
            return false;
        }

        for (int i = 0; i < length; i++)
            if (data[i] != thatData[i]) {
                return false;
            }

        return true;
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
