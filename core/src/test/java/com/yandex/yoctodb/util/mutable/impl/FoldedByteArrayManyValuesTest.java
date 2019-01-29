package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.DatabaseFormat;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.STORED;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FoldedByteArrayManyValuesTest {
    private String fieldName = "test";
    @Test
    public void string() throws IOException {
        testDB(10);
        testDB(300);

    }

    private void testDB(final int size) throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        for (int i = 0; i < size; i++) {
            dbBuilder.merge(buildTestDocument(Integer.toString((i % 3))));
        }
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));
        for (int i = 0; i < size; i++) {
            assertEquals(Integer.toString((i % 3)),
                    UnsignedByteArrays.toString(
                            UnsignedByteArrays.from(db.getFoldedFieldValue(i, fieldName))));
        }
    }

    private DocumentBuilder buildTestDocument(String value) {
        return DatabaseFormat.getCurrent().newDocumentBuilder()
                .withField(fieldName, value, STORED);
    }

//    @Test
//    public void checkOutputStream() throws IOException {
//        List<UnsignedByteArray> list = initString();
//        final FoldedByteArrayIndexedList set =
//                new FoldedByteArrayIndexedList(list);
//    }
}
