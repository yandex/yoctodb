package com.yandex.yoctodb.util.common;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.NoSuchElementException;

public class BufferIterator {
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

    public boolean hasNext() {
        return advance < remaining;
    }

    public int next() {
        if (hasNext()) {
            return Byte.toUnsignedInt(buffer.get(advance++));
        } else {
            throw new NoSuchElementException("Empty iterator");
        }
    }

    public static int strip(@NotNull BufferIterator from, @NotNull BufferIterator elements) {
        while (from.hasNext() && elements.hasNext()) {
            int result = Integer.compare(from.next(), elements.next());
            if (result != 0) {
                return result;
            }
        }

        return elements.hasNext() ? -1 : 0;
    }

    public static BufferIterator wrapCopy(final Collection<Byte> bytes) {
        final ByteBuffer allocated = ByteBuffer.allocate(bytes.size());
        for (Byte b : bytes) {
            allocated.put(b);
        }
        allocated.rewind();

        final Buffer buf = Buffer.from(allocated);
        return new BufferIterator(buf);
    }
}
