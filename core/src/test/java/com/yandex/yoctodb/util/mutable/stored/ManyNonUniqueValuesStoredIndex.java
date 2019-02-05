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

public class ManyNonUniqueValuesStoredIndex {
    private String fieldName = "test";
    @Test
    public void oneByte() throws IOException {
        testDB(100);
    }
    @Test
    public void twoByte() throws IOException {
        testDB(300);
        testDB(1000);
        testDB(2000);
        /*
        Result:
        Write 290 values in 10 ms
        Read database in 76 ms
        Read 10 values in 0 ms
        Read 290 in 3 ms
         */
    }

    @Test
    public void fourByte() throws IOException {
        testDB(70000);
        testDB(69999);
        testDB(70001);
        /*
        Result:
        Write 69990 values in 265 ms
        Read database in 53 ms
        Read and compare 10 values in 0 ms
        Read and compare 69990 in 56 ms
        */
    }

    private void testDB(final int size) throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();
        // init first 10 elements by the same values
        for (int i = 0; i < 10; i++) {
            dbBuilder.merge(buildTestDocument(Integer.toString((0))));
        }
        long start = System.currentTimeMillis();
        for (int i = 10; i < size; i++) {
            dbBuilder.merge(buildTestDocument(Integer.toString((i))));
        }
        long end = System.currentTimeMillis();
        System.out.println("Write " + (size - 10) +
                " values in " +
                (end - start) + " ms");
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        start = System.currentTimeMillis();
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));
        end = System.currentTimeMillis();
        System.out.println("Read database in " + (end - start) + " ms");

        start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            assertEquals(Integer.toString((0)),
                    UnsignedByteArrays.toString(
                            UnsignedByteArrays.from(db.getFieldValue(i, fieldName))));
        }
        end = System.currentTimeMillis();

        System.out.println("Read and compare 10 values in " +
                (end - start) + " ms");

        start = System.currentTimeMillis();
        for (int i = 10; i < 100; i++) {
            assertEquals(Integer.toString((i)),
                    UnsignedByteArrays.toString(
                            UnsignedByteArrays.from(db.getFieldValue(i, fieldName))));
        }
        end = System.currentTimeMillis();
        System.out.println("Read and compare " + (size - 10) + " in " +
                (end - start) + " ms");
    }

    private DocumentBuilder buildTestDocument(String value) {
        return DatabaseFormat.getCurrent().newDocumentBuilder()
                .withField(fieldName, value, STORED);
    }
}
