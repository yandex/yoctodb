/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb;

import com.yandex.yoctodb.immutable.DocumentProvider;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption;
import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link DocumentProvider#getFieldValue(int, String)}
 *
 * @author incubos
 */
public class FieldValueExtractionTest {
    private static final String FIELD_NAME = "id";
    private static final String INT_FIELD_NAME = "int_id";
    private static final String LONG_FIELD_NAME = "long_id";

    private DocumentProvider build(
            final IndexOption indexOption) throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                FIELD_NAME,
                                0,
                                indexOption)
                        .withField(INT_FIELD_NAME, 25, indexOption)
                        .withField(LONG_FIELD_NAME, 25L, indexOption)
                        .withPayload("payload".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        return DatabaseFormat.getCurrent()
                .getDatabaseReader()
                .from(Buffer.from(os.toByteArray()));
    }

    @Test
    public void extractFullFieldValue() throws IOException {
        final DocumentProvider db = build(IndexOption.FULL);

        assertEquals(from(0).toByteBuffer(), db.getFieldValue(0, FIELD_NAME));
    }

    @Test
    public void extractSortableFieldValue() throws IOException {
        final DocumentProvider db = build(IndexOption.SORTABLE);

        assertEquals(from(0).toByteBuffer(), db.getFieldValue(0, FIELD_NAME));
    }

    @Test
    public void extractStoredFieldValue() throws IOException {
        final DocumentProvider db = build(IndexOption.STORED);
        assertEquals(from(0).toByteBuffer(), db.getFieldValue(0, FIELD_NAME));
    }

    @Test
    public void extractStoredLong() throws IOException {
        final DocumentProvider db = build(IndexOption.STORED);
        final long storedValue = from(25L).toByteBuffer().getLong() ^ Long.MIN_VALUE;
        assertEquals(storedValue, db.getLongValue(0, LONG_FIELD_NAME));
    }

    @Test
    public void extractStoredInt() throws IOException {
        final DocumentProvider db = build(IndexOption.STORED);
        final long storedValue = from(25).toByteBuffer().getInt() ^ Integer.MIN_VALUE;
        assertEquals(storedValue, db.getIntValue(0, INT_FIELD_NAME));
    }

    @Test(expected = AssertionError.class)
    public void notExtractFilterableFieldValue() throws IOException {
        final DocumentProvider db = build(IndexOption.FILTERABLE);

        db.getFieldValue(0, FIELD_NAME);
    }

    @Test(expected = AssertionError.class)
    public void notExtractNonExistingFieldValue() throws IOException {
        final DocumentProvider db = build(IndexOption.SORTABLE);

        db.getFieldValue(0, "field");
    }

    @Test(expected = AssertionError.class)
    public void notExtractNonExistingDocumentFieldValue() throws IOException {
        final DocumentProvider db = build(IndexOption.SORTABLE);

        db.getFieldValue(1, FIELD_NAME);
    }
}
