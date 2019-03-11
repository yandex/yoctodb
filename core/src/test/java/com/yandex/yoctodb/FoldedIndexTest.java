package com.yandex.yoctodb;

import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.util.UnsignedByteArray;
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
public class FoldedIndexTest {

    @Test
    public void buildDatabase() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("state", "NEW", STORED)
                        .withField("byte", (byte) 1, STORED)
                        .withField("short", (short) 1, STORED)
                        .withField("int", 1, STORED)
                        .withField("long", 1L, STORED)
                        .withField("char", 'a', STORED)
        );
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("state", "USED", STORED)
                        .withField("byte", (byte) 2, STORED)
                        .withField("short", (short) 2, STORED)
                        .withField("int", 2, STORED)
                        .withField("long", 2L, STORED)
                        .withField("char", 'b', STORED)
        );
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("state", "NEW", STORED)
                        .withField("byte", (byte) 1, STORED)
                        .withField("short", (short) 1, STORED)
                        .withField("int", 1, STORED)
                        .withField("long", 1L, STORED)
                        .withField("char", 'a', STORED)
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        assertEquals("NEW", getValueFromBuffer(db.getFieldValue(0, "state")));
        assertEquals("USED", getValueFromBuffer(db.getFieldValue(1, "state")));
        assertEquals("NEW", getValueFromBuffer(db.getFieldValue(2, "state")));

        assertEquals((byte) 1, db.getByteValue(0, "byte"));
        assertEquals((byte) 2, db.getByteValue(1, "byte"));
        assertEquals((byte) 1, db.getByteValue(2, "byte"));

        assertEquals((short) 1, db.getShortValue(0, "short"));
        assertEquals((short) 2, db.getShortValue(1, "short"));
        assertEquals((short) 1, db.getShortValue(2, "short"));

        assertEquals(1, db.getIntValue(0, "int"));
        assertEquals(2, db.getIntValue(1, "int"));
        assertEquals(1, db.getIntValue(2, "int"));

        assertEquals(1L, db.getLongValue(0, "long"));
        assertEquals(2L, db.getLongValue(1, "long"));
        assertEquals(1L, db.getLongValue(2, "long"));

        assertEquals('a', db.getCharValue(0, "char"));
        assertEquals('b', db.getCharValue(1, "char"));
        assertEquals('a', db.getCharValue(2, "char"));


    }

    private String getValueFromBuffer(Buffer buffer) {
        UnsignedByteArray byteArray = UnsignedByteArrays.from(buffer);
        return UnsignedByteArrays.toString(byteArray);
    }
}
