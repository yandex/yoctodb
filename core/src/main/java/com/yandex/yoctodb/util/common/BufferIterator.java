/*
 * (C) YANDEX LLC, 2014-2018
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.common;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.NoSuchElementException;

public final class BufferIterator {
    @NotNull
    private final Buffer buffer;
    private final long limit;
    private long position;

    public BufferIterator(@NotNull Buffer buffer) {
        this.buffer = buffer;
        this.position = buffer.position();
        this.limit = this.position + buffer.remaining();
    }

    public BufferIterator(@NotNull Buffer buffer, long offset, long size) {
        this.buffer = buffer;
        this.position = offset;
        this.limit = Math.min(buffer.limit(), offset + size);
    }

    public final boolean hasNext() {
        return position < limit;
    }

    public final int next() {
        if (hasNext()) {
            return Byte.toUnsignedInt(buffer.get(position++));
        } else {
            throw new NoSuchElementException("Empty iterator");
        }
    }

    /**
     * Strips equal bytes of this iterator by {@code prefix} iterator.
     *
     * @param prefix iterator that contains prefix
     * @return {@code 0} if this iterator was equal to prefix or larger in length,
     *         negative value if it was shorter or lexicographically smaller than {@code prefix}
     *         positive value if it was lexicographically larger than {@code prefix}
     */
    public final int compareToPrefix(@NotNull BufferIterator prefix) {
        while (this.hasNext() && prefix.hasNext()) {
            int result = Integer.compare(this.next(), prefix.next());
            if (result != 0) {
                return result;
            }
        }

        return prefix.hasNext() ? -1 : 0;
    }

    public static BufferIterator wrapCopy(@NotNull final Collection<Byte> bytes) {
        final ByteBuffer allocated = ByteBuffer.allocate(bytes.size());
        for (Byte b : bytes) {
            allocated.put(b);
        }
        allocated.rewind();

        final Buffer buf = Buffer.from(allocated);
        return new BufferIterator(buf);
    }
}
