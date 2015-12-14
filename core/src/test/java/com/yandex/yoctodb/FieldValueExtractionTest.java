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
