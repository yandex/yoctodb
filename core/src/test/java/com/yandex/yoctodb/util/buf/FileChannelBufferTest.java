/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.buf;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import static org.mockito.Mockito.*;

/**
 * Tests in {@link com.yandex.yoctodb.util.buf.FileChannelBuffer}
 *
 * @author dimas
 */
public class FileChannelBufferTest extends BufferTest {

    private final AtomicInteger fileNumber = new AtomicInteger();

    @Override
    protected Buffer bufferOf(byte[] data) {
        try {
            final Path path = nextTempFile(data);

            final FileChannel read = FileChannel.open(
                    path,
                    StandardOpenOption.READ,
                    StandardOpenOption.DELETE_ON_CLOSE);

            return Buffer.from(read);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path nextTempFile(byte[] data) throws IOException {
        final Path path = createTempFile().toPath();
        final FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        channel.write(ByteBuffer.wrap(data));
        channel.close();
        return path;
    }

    private File createTempFile() throws IOException {
        final String suffix = String.valueOf(
                fileNumber.incrementAndGet());
        return File.createTempFile(
                "file_channel_buffer_test_",
                suffix);
    }

    @Test(expected = RuntimeException.class)
    public void brokenSize() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken);
    }

    @Test(expected = RuntimeException.class)
    public void brokenSizeWithOffset() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size())
                .thenReturn(1024L)
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).slice(0L, 1024L);
    }

    @Test(expected = RuntimeException.class)
    public void getByte() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).get();
    }

    @Test(expected = RuntimeException.class)
    public void getBuffer() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).get(new byte[1]);
    }

    @Test(expected = RuntimeException.class)
    public void getSubBuffer() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).get(new byte[1], 0, 1);
    }

    @Test(expected = RuntimeException.class)
    public void getByteByIndex() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).get(0L);
    }

    @Test(expected = RuntimeException.class)
    public void getInt() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).getInt();
    }

    @Test(expected = RuntimeException.class)
    public void getIntByIndex() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).getInt(0L);
    }

    @Test(expected = RuntimeException.class)
    public void getLong() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).getLong();
    }

    @Test(expected = RuntimeException.class)
    public void getLongByIndex() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).getLong(0L);
    }

    @Test(expected = RuntimeException.class)
    public void getShort() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).getShort();
    }

    @Test(expected = RuntimeException.class)
    public void getShortByIndex() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).getShort(0L);
    }

    @Test(expected = RuntimeException.class)
    public void getChar() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).getChar();
    }

    @Test(expected = RuntimeException.class)
    public void getCharByIndex() throws IOException {
        final FileChannel broken = mock(FileChannel.class);
        when(broken.size()).thenReturn(1024L);
        when(broken.read(any(ByteBuffer.class), anyLong()))
                .thenThrow(new IOException("Test"));
        new FileChannelBuffer(broken).getChar(0L);
    }
}
