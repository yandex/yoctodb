package com.yandex.yoctodb.util.buf;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.InputStream;

/**
 * {@link java.io.InputStream} implementation wrapping {@link com.yandex.yoctodb.util.buf.Buffer}
 *
 * @author dimas
 * @author incubos
 */
@NotThreadSafe
public final class BufferInputStream extends InputStream {
    @NotNull
    private final Buffer buf;

    public BufferInputStream(
            @NotNull
            final Buffer buf) {
        this.buf = buf;
    }

    public int read() {
        if (buf.hasRemaining()) {
            return buf.get() & 0xFF;
        } else {
            return -1;
        }
    }

    public int read(
            @NotNull
            final byte[] bytes,
            final int off,
            final int len) {
        if (buf.hasRemaining()) {
            final int remaining =
                    buf.remaining() > Integer.MAX_VALUE ?
                            Integer.MAX_VALUE :
                            (int) buf.remaining();
            final int toRead = Math.min(len, remaining);

            buf.get(bytes, off, toRead);

            return toRead;
        } else {
            return -1;
        }
    }
}