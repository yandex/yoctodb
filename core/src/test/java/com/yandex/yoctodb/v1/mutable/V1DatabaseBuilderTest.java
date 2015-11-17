/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable;

import com.yandex.yoctodb.DatabaseFormat;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.security.NoSuchAlgorithmException;

import static com.yandex.yoctodb.v1.V1DatabaseFormat.*;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link V1DatabaseBuilder}
 *
 * @author incubos
 */
public class V1DatabaseBuilderTest {
    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedIndexOption() {
        final DocumentBuilder doc =
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "k",
                                "v",
                                DocumentBuilder.IndexOption.UNSUPPORTED)
                        .withPayload("payload".getBytes());

        DatabaseFormat.getCurrent().newDatabaseBuilder().merge(doc);
    }

    @Test(expected = IllegalStateException.class)
    public void frozenDocumentModification() {
        final DocumentBuilder doc =
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "k",
                                "v",
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withPayload("payload".getBytes());

        DatabaseFormat.getCurrent().newDatabaseBuilder().merge(doc);

        doc.withField("extra", "value", DocumentBuilder.IndexOption.SORTABLE);
    }

    @Test(expected = IllegalStateException.class)
    public void frozenDatabaseModification() {
        final DocumentBuilder doc1 =
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "id",
                                0,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withPayload("payload1".getBytes());

        final DatabaseBuilder db =
                DatabaseFormat.getCurrent().newDatabaseBuilder().merge(doc1);

        db.buildWritable();

        final DocumentBuilder doc2 =
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "id",
                                1,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withPayload("payload2".getBytes());

        db.merge(doc2);
    }

    @Test
    public void getSize() {
        final DocumentBuilder doc1 =
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "id",
                                0,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload1".getBytes());

        final DocumentBuilder doc2 =
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "id",
                                1,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload2".getBytes());

        final long size1 =
                DatabaseFormat.getCurrent().newDatabaseBuilder()
                        .merge(doc1)
                        .buildWritable()
                        .getSizeInBytes();

        final long size2 =
                DatabaseFormat.getCurrent().newDatabaseBuilder()
                        .merge(doc1)
                        .merge(doc2)
                        .buildWritable()
                        .getSizeInBytes();

        assertTrue(size1 < size2);
    }

    @Test(expected = NoSuchAlgorithmException.class)
    public void wrongDigestAlgorithm() throws Throwable {
        final DocumentBuilder doc =
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "id",
                                0,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload1".getBytes());


        final DatabaseBuilder db =
                DatabaseFormat.getCurrent().newDatabaseBuilder().merge(doc);

        final String originalAlgorithm = getMessageDigestAlgorithm();
        try {
            setMessageDigestAlgorithm("WRONG");

            db.buildWritable().writeTo(new ByteArrayOutputStream());
        } catch (Exception e) {
            throw e.getCause();
        } finally {
            setMessageDigestAlgorithm(originalAlgorithm);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void wrongDigestLength() throws Throwable {
        final DocumentBuilder doc =
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "id",
                                0,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload1".getBytes());


        final DatabaseBuilder db =
                DatabaseFormat.getCurrent().newDatabaseBuilder().merge(doc);

        final int originalDigestSize = getDigestSizeInBytes();
        try {
            setDigestSizeInBytes(originalDigestSize * 2);

            db.buildWritable().writeTo(new ByteArrayOutputStream());
        } finally {
            setDigestSizeInBytes(originalDigestSize);
        }
    }
}
