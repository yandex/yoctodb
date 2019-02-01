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

public class ManyUniqueValuesStoredIndex {
    private String fieldName = "test";
    @Test
    public void values100() throws IOException {
        testDB(100);
    }

    @Test
    public void values300() throws IOException {
        testDB(300);
        /*
        Result:
        Write 300 values in 41 ms
        Read database in 12 ms
        Read 300 values in 5 ms
         */
    }

//    @Test
//    public void values70000() throws IOException {
//        testDB(70000);
//        /*
//        Result
//        Write 70000 values in 321 ms
//        Read database in 12 ms
//        Read 70000 values56 ms
//         */
//    }

    private void testDB(final int size) throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();
        long start = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            dbBuilder.merge(buildTestDocument(Integer.toString((i))));
        }
        long end = System.currentTimeMillis();
        System.out.println("Write " + size +
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
        for (int i = 0; i < size; i++) {
            assertEquals(Integer.toString((i)),
                    UnsignedByteArrays.toString(
                            UnsignedByteArrays.from(db.getFieldValue(i, fieldName))));
        }
        end = System.currentTimeMillis();
        System.out.println("Read " + size + " values in " +
                (end - start) + " ms");

    }

    private DocumentBuilder buildTestDocument(String value) {
        return DatabaseFormat.getCurrent().newDocumentBuilder()
                .withField(fieldName, value, STORED);
    }
}

