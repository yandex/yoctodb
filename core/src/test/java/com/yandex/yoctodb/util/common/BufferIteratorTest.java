package com.yandex.yoctodb.util.common;

import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class BufferIteratorTest {
    @Test(expected = NoSuchElementException.class)
    public void invalidNext() {
        new BufferIterator(Buffer.from(ByteBuffer.allocate(0))).next();
    }
}