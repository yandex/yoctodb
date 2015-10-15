package com.yandex.yoctodb.util.buf;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

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
}
