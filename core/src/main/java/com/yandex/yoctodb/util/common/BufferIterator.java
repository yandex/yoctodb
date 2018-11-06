package com.yandex.yoctodb.util.common;

import com.google.common.primitives.UnsignedBytes;
import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class BufferIterator implements Iterator<Byte> {
    @NotNull
    private final Buffer buffer;
    private final long remaining;
    private long advance;

    public BufferIterator(@NotNull Buffer buffer) {
        this.buffer = buffer;
        this.advance = buffer.position();
        this.remaining = buffer.remaining();
    }

    public BufferIterator(@NotNull Buffer buffer, long offset, long size) {
        this.buffer = buffer;
        this.advance = offset;
        this.remaining = Math.min(buffer.limit(), offset + size);
    }

    @Override
    public boolean hasNext() {
        return advance < remaining;
    }

    @Override
    public Byte next() {
        if (hasNext()) {
            return buffer.get(advance++);
        } else {
            throw new NoSuchElementException("Empty iterator");
        }
    }

    public static int compareMutableIterators(@NotNull Iterator<Byte> left, @NotNull Iterator<Byte> right) {
        while (left.hasNext() && right.hasNext()) {
            int result = UnsignedBytes.compare(left.next(), right.next());
            if (result != 0) {
                return result;
            }
        }

        return Boolean.compare(left.hasNext(), right.hasNext());
    }
}
