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
                        .withField("state", "NEW", STORED));
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("state", "USED", STORED));
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("state", "NEW", STORED));

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        assertEquals("NEW",getValueFromBuffer(db.getFoldedFieldValue(0, "state")));
        assertEquals("USED",getValueFromBuffer(db.getFoldedFieldValue(1, "state")));
        assertEquals("NEW",getValueFromBuffer(db.getFoldedFieldValue(2, "state")));
    }

    private String getValueFromBuffer(Buffer buffer) {
        UnsignedByteArray byteArray = UnsignedByteArrays.from(buffer);
        return UnsignedByteArrays.toString(byteArray);
    }

}
