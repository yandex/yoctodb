package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.impl.*;

import java.io.*;

public class IndexToIndexMultiMapBenchmark {
    private static final BitSetIndexToIndexMultiMap bitSetIndex;
    private static final IntIndexToIndexMultiMap intToIntIndex;
    private static final AscendingBitSetIndexToIndexMultiMap accumulatedIndex;

    private static Buffer persist(
            final com.yandex.yoctodb.util.OutputStreamWritable writable) {
        final File file;
        try {
            file = File.createTempFile("fixed", ".yoctodb");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        file.deleteOnExit();

        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
            writable.writeTo(os);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            return Buffer.mmap(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        bitSetIndex = null;
        intToIntIndex = null;
        accumulatedIndex = null;
    }
}
