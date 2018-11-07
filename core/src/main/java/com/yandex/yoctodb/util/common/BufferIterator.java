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

    public static int strip(@NotNull Iterator<Byte> from, @NotNull Iterator<Byte> elements) {
        while (from.hasNext() && elements.hasNext()) {
            int result = UnsignedBytes.compare(from.next(), elements.next());
            if (result != 0) {
                return result;
            }
        }

        return elements.hasNext() ? -1 : 0;
    }
}
