package com.yandex.yoctodb.util.mutable.stored;

import com.yandex.yoctodb.DatabaseFormat;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.STORED;
import static org.junit.Assert.assertEquals;

/**
 * {@link com.yandex.yoctodb.util.immutable.ByteArrayIndexedList} with fixed size
 * elements
 *
 * @author irenkamalova
 */
public class ManyUniqueValuesTest {
    private String fieldName = "test";
    @Test
    public void values100() throws IOException {
        testDB(100);
    }

    @Test
    public void values300() throws IOException {
        testDB(300);
    }

    @Test
    public void values70000() throws IOException {
        testDB(70000);
    }

    private void testDB(final int size) throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();
        for (int i = 0; i < size; i++) {
            dbBuilder.merge(buildTestDocument(Integer.toString((i))));
        }
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        for (int i = 0; i < 100; i++) {
            assertEquals(Integer.toString((i)),
                    UnsignedByteArrays.toString(
                            UnsignedByteArrays.from(db.getFieldValue(i, fieldName))));
        }
    }

    private DocumentBuilder buildTestDocument(String value) {
        return DatabaseFormat.getCurrent().newDocumentBuilder()
                .withField(fieldName, value, STORED);
    }
}

