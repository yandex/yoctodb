/*
 * (C) YANDEX LLC, 2014-2019
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

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

    String BYTE_FIELD_NAME = "byte";
    String SHORT_FIELD_NAME = "short";
    String INT_FIELD_NAME = "int";
    String LONG_FIELD_NAME = "long";
    String CHAR_FIELD_NAME = "char";
    String STATE_FIELD_NAME = "state";

    @Test
    public void buildDatabase() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(STATE_FIELD_NAME, "NEW", STORED)
                        .withField(BYTE_FIELD_NAME, (byte) 1, STORED)
                        .withField(SHORT_FIELD_NAME, (short) 1, STORED)
                        .withField(INT_FIELD_NAME, 1, STORED)
                        .withField(LONG_FIELD_NAME, 1L, STORED)
                        .withField(CHAR_FIELD_NAME, 'a', STORED)
        );
        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(STATE_FIELD_NAME, "USED", STORED)
                        .withField(BYTE_FIELD_NAME, (byte) 2, STORED)
                        .withField(SHORT_FIELD_NAME, (short) 2, STORED)
                        .withField(INT_FIELD_NAME, 2, STORED)
                        .withField(LONG_FIELD_NAME, 2L, STORED)
                        .withField(CHAR_FIELD_NAME, 'b', STORED)
        );
        // Document 3
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(STATE_FIELD_NAME, "NEW", STORED)
                        .withField(BYTE_FIELD_NAME, (byte) 1, STORED)
                        .withField(SHORT_FIELD_NAME, (short) 1, STORED)
                        .withField(INT_FIELD_NAME, 1, STORED)
                        .withField(LONG_FIELD_NAME, 1L, STORED)
                        .withField(CHAR_FIELD_NAME, 'a', STORED)
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        assertEquals("NEW", getValueFromBuffer(db.getFieldValue(0, STATE_FIELD_NAME)));
        assertEquals("USED", getValueFromBuffer(db.getFieldValue(1, STATE_FIELD_NAME)));
        assertEquals("NEW", getValueFromBuffer(db.getFieldValue(2, STATE_FIELD_NAME)));

        assertEquals((byte) 1, db.getByteValue(0, BYTE_FIELD_NAME));
        assertEquals((byte) 2, db.getByteValue(1, BYTE_FIELD_NAME));
        assertEquals((byte) 1, db.getByteValue(2, BYTE_FIELD_NAME));

        assertEquals((short) 1, db.getShortValue(0, SHORT_FIELD_NAME));
        assertEquals((short) 2, db.getShortValue(1, SHORT_FIELD_NAME));
        assertEquals((short) 1, db.getShortValue(2, SHORT_FIELD_NAME));

        assertEquals(1, db.getIntValue(0, INT_FIELD_NAME));
        assertEquals(2, db.getIntValue(1, INT_FIELD_NAME));
        assertEquals(1, db.getIntValue(2, INT_FIELD_NAME));

        assertEquals(1L, db.getLongValue(0, LONG_FIELD_NAME));
        assertEquals(2L, db.getLongValue(1, LONG_FIELD_NAME));
        assertEquals(1L, db.getLongValue(2, LONG_FIELD_NAME));

        assertEquals('a', db.getCharValue(0, CHAR_FIELD_NAME));
        assertEquals('b', db.getCharValue(1, CHAR_FIELD_NAME));
        assertEquals('a', db.getCharValue(2, CHAR_FIELD_NAME));


    }

    private String getValueFromBuffer(Buffer buffer) {
        UnsignedByteArray byteArray = UnsignedByteArrays.from(buffer);
        return UnsignedByteArrays.toString(byteArray);
    }
}
