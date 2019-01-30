package com.yandex.yoctodb.util.mutable.impl;

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

public class FoldedByteArrayManyValuesTest {
    private String fieldName = "test";
    @Test
    public void oneByte() throws IOException {
        testDB(100);
    }
    @Test
    public void twoByte() throws IOException {
        testDB(300);

    }

    @Test
     // отрабатывает за минуту
    public void threeByte() throws IOException {
        testDB(70000);
    }

//    @Test
    // слишком долго работает
//    public void fourByte() throws IOException {
//        // attention - a really heavy test
//        testDB(17777215);
//    }

    private void testDB(final int size) throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();
        // инициализируем первые 10 элементов одинаковым значением
        for (int i = 0; i < 10; i++) {
            dbBuilder.merge(buildTestDocument(Integer.toString((0))));
        }
        for (int i = 10; i < size; i++) {
            dbBuilder.merge(buildTestDocument(Integer.toString((i))));
        }
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));
        for (int i = 0; i < 10; i++) {
            assertEquals(Integer.toString((0)),
                    UnsignedByteArrays.toString(
                            UnsignedByteArrays.from(db.getFieldValue(i, fieldName))));
        }
        for (int i = 11; i < size; i++) {
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
