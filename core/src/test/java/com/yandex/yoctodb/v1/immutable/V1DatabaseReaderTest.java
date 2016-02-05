/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable;

import com.yandex.yoctodb.DatabaseFormat;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import static com.yandex.yoctodb.DatabaseFormat.MAGIC;
import static com.yandex.yoctodb.v1.V1DatabaseFormat.*;

/**
 * Unit tests for {@link V1DatabaseReader}
 *
 * @author incubos
 */
public class V1DatabaseReaderTest {
    private final V1DatabaseReader INSTANCE = new V1DatabaseReader();

    private byte[] buildDatabase() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "text",
                                "doc1234",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                1,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "text",
                                "doc2",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                2,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        return os.toByteArray();
    }

    @Test(expected = NoSuchAlgorithmException.class)
    public void wrongDigestAlgorithm() throws Throwable {
        final Buffer buffer = Buffer.from(buildDatabase());
        final String originalAlgorithm = getMessageDigestAlgorithm();
        final int originalSize = getDigestSizeInBytes();
        try {
            setMessageDigestAlgorithm("WRONG");
            INSTANCE.from(buffer);
        } catch (Exception e) {
            throw e.getCause();
        } finally {
            setDigestSizeInBytes(originalSize);
            setMessageDigestAlgorithm(originalAlgorithm);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongMagic() throws IOException {
        final byte[] bytes = buildDatabase();
        bytes[0] = (byte) ~MAGIC[0];
        INSTANCE.from(Buffer.from(bytes));
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongFormat() throws IOException {
        final byte[] bytes = buildDatabase();
        bytes[MAGIC.length] = (byte) ~bytes[MAGIC.length];
        INSTANCE.from(Buffer.from(bytes));
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmall() throws IOException {
        final byte[] bytes =
                Arrays.copyOf(
                        buildDatabase(),
                        MAGIC.length + getDigestSizeInBytes() - 1);
        INSTANCE.from(Buffer.from(bytes));
    }
}
