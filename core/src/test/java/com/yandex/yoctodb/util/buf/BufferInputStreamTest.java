package com.yandex.yoctodb.util.buf;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link com.yandex.yoctodb.util.buf.BufferInputStream}
 *
 * @author incubos
 */
public class BufferInputStreamTest {
    private static final byte[] DATA = {0, 1, 2, 3};

    @Test
    public void readByte() throws IOException {
        final InputStream is = new BufferInputStream(Buffer.from(DATA));

        for (final byte aDATA : DATA) assertEquals(aDATA, is.read());

        assertEquals(-1, is.read());
    }

    @Test
    public void readBuffer() throws IOException {
        final InputStream is = new BufferInputStream(Buffer.from(DATA));
        final byte[] buf = new byte[DATA.length];

        assertEquals(buf.length, is.read(buf));

        assertArrayEquals(DATA, buf);

        assertEquals(-1, is.read(buf));
    }
}
