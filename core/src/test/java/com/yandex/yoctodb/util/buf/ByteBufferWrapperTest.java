package com.yandex.yoctodb.util.buf;

/**
 * Tests for {@link com.yandex.yoctodb.util.buf.ByteBufferWrapper}
 *
 * @author dimas
 */
public class ByteBufferWrapperTest extends BufferTest {
    @Override
    protected Buffer bufferOf(byte[] data) {
        return Buffer.from(data);
    }
}
