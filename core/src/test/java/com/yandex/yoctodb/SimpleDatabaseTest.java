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

import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.simple.SimpleDescendingOrder;
import com.yandex.yoctodb.query.simple.SimpleRangeCondition;
import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.FILTERABLE;
import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.FULL;
import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.SORTABLE;
import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.STORED;
import static com.yandex.yoctodb.query.QueryBuilder.asc;
import static com.yandex.yoctodb.query.QueryBuilder.desc;
import static com.yandex.yoctodb.query.QueryBuilder.eq;
import static com.yandex.yoctodb.query.QueryBuilder.gt;
import static com.yandex.yoctodb.query.QueryBuilder.gte;
import static com.yandex.yoctodb.query.QueryBuilder.in;
import static com.yandex.yoctodb.query.QueryBuilder.lt;
import static com.yandex.yoctodb.query.QueryBuilder.lte;
import static com.yandex.yoctodb.query.QueryBuilder.not;
import static com.yandex.yoctodb.query.QueryBuilder.or;
import static com.yandex.yoctodb.query.QueryBuilder.select;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for building and searching a simple database
 *
 * @author incubos
 */
public class SimpleDatabaseTest {
    private final int DOCS = 128;

    private final String LONG_STORED_FIELD_NAME = "long_stored";
    private final String LONG_FULL_FIELD_NAME = "long_full";
    private final String LONG_SORTABLE_FIELD_NAME = "long_sortable";
    private final long LONG_FIELD_VALUE = 25L;

    private final String INT_STORED_FIELD_NAME = "int_stored";
    private final String INT_FULL_FIELD_NAME = "int_full";
    private final String INT_SORTABLE_FIELD_NAME = "int_sortable";
    private final int INT_FIELD_VALUE = 15;

    private final String SHORT_STORED_FIELD_NAME = "short_stored";
    private final String SHORT_FULL_FIELD_NAME = "short_full";
    private final String SHORT_SORTABLE_FIELD_NAME = "short_sortable";
    private final short SHORT_FIELD_VALUE = 13;

    private final String CHAR_STORED_FIELD_NAME = "char_stored";
    private final String CHAR_FULL_FIELD_NAME = "char_full";
    private final String CHAR_SORTABLE_FIELD_NAME = "char_sortable";
    private final char CHAR_FIELD_VALUE = 'a';

    private final String BYTE_STORED_FIELD_NAME = "byte_stored";
    private final String BYTE_FULL_FIELD_NAME = "byte_full";
    private final String BYTE_SORTABLE_FIELD_NAME = "byte_sortable";
    private final byte BYTE_FIELD_VALUE = -128;

    @Test
    public void buildDatabase() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc1234", FULL)
                        .withField("int", 1, FULL)
                        .withField("payload", from("payload2"), STORED)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc2", FULL)
                        .withField("int", 2, FULL)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));
        final Query q1 = select().where(eq("int", from(1)));
        assertTrue(db.count(q1) == 1);
        final Query q2 = select().where(eq("int", from(2)));
        assertTrue(db.count(q2) == 1);

        final Query q3 = select().where(eq("int", from(2)))
                .and(in("int", from(2),
                        from(1)));
        assertTrue(db.count(q3) == 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void detectCorruptedDatabase() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc1234", FULL)
                        .withField("int", 1, FULL)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc2", FULL)
                        .withField("int", 2, FULL)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final byte[] data = os.toByteArray();
        // Corruption
        data[data.length / 2] = (byte) ~data[data.length / 2];
        DatabaseFormat.getCurrent()
                .getDatabaseReader()
                .from(Buffer.from(data));
    }

    @Test
    public void buildWithAllSupportedDataTypes() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                                //string
                        .withField("string_field_full", "string_1", FULL)
                        .withField("string_field_filterable", "string_1", FILTERABLE)
                        .withField("string_field_sortable", "string_1", SORTABLE)
                                //byte
                        .withField("byte_field_full", (byte) 1, FULL)
                        .withField("byte_field_filterable", (byte) 1, FILTERABLE)
                        .withField("byte_field_sortable", (byte) 1, SORTABLE)
                                //short
                        .withField("short_field_full", (short) 1, FULL)
                        .withField("short_field_filterable", (short) 1, FILTERABLE)
                        .withField("short_field_sortable", (short) 1, SORTABLE)
                                //integer
                        .withField("int_field_full", 1, FULL)
                        .withField("int_field_filterable", 1, FILTERABLE)
                        .withField("int_field_sortable", 1, SORTABLE)
                                //long
                        .withField("long_field_full", 1L, FULL)
                        .withField("long_field_filterable", 1L, FILTERABLE)
                        .withField("long_field_sortable", 1L, SORTABLE)
                                //boolean
                        .withField("boolean_field_full", false, FULL)
                        .withField("boolean_field_filterable", false, FILTERABLE)
                        .withField("boolean_field_sortable", false, SORTABLE)
                                //
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("string_field_full", "doc2", FULL)
                        .withField("int_field_full", 2, FULL)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Collection<Integer> docs = new LinkedList<>();
        final DocumentProcessor processor =
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            @NotNull
                            final Database database) {
                        docs.add(document);
                        return true;
                    }
                };

        final Query q1 =
                select().where(eq("int_field_full", from(1)));

        assertTrue(db.count(q1) == 1);

        db.execute(q1, processor);
        assertEquals(Collections.singletonList(0), docs);

        docs.clear();
        assertEquals(1, db.executeAndUnlimitedCount(q1, processor));
        assertEquals(Collections.singletonList(0), docs);

        final Query q2 =
                select().where(eq("string_field_full", from("doc2")));

        assertTrue(db.count(q2) == 1);

        docs.clear();
        db.execute(q2, processor);
        assertEquals(Collections.singletonList(1), docs);

        docs.clear();
        assertEquals(1, db.executeAndUnlimitedCount(q2, processor));
        assertEquals(Collections.singletonList(1), docs);
    }

    @Test
    public void testWithNoContinuousIndexes() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc1234", FILTERABLE)
                        .withField("text", "doc1", FILTERABLE)
                        .withField("doc2_not_contain_this_field", "doc2", FILTERABLE)
                        .withField("doc2_not_contain_this_field2", 1, FILTERABLE)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc2", FILTERABLE)
                        .withField("int", 2, FILTERABLE)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Collection<Integer> docs = new LinkedList<>();
        final DocumentProcessor processor =
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            @NotNull
                            final Database database) {
                        docs.add(document);
                        return true;
                    }
                };

        final Query q1 = select().where(eq("int", from(2)));

        assertTrue(db.count(q1) == 1);

        db.execute(q1, processor);
        assertEquals(Collections.singletonList(1), docs);

        docs.clear();
        assertEquals(1, db.executeAndUnlimitedCount(q1, processor));
        assertEquals(Collections.singletonList(1), docs);

        final Query q2 = select().where(eq("int", from(2)))
                .and(
                        in(
                                "int",
                                from(2),
                                from(1)));

        assertTrue(db.count(q2) == 1);

        docs.clear();
        db.execute(q2, processor);
        assertEquals(Collections.singletonList(1), docs);

        docs.clear();
        assertEquals(1, db.executeAndUnlimitedCount(q2, processor));
        assertEquals(Collections.singletonList(1), docs);
    }

    @Test
    public void countTest() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            dbBuilder.merge(
                    DatabaseFormat
                            .getCurrent()
                            .newDocumentBuilder()
                            .withField( "id", i, FULL)
                            .withPayload(("payload" + i).getBytes())
            );
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        //less
        for (int i = 0; i < DOCS; i++) {
            final Query q1 = select().where(lt("id", from(i)));
            assertEquals(i, db.count(q1));
        }

        //less or equals
        for (int i = 0; i < DOCS; i++) {
            final Query q1 = select().where(lte("id", from(
                    i)));
            assertEquals(i + 1, db.count(q1));
        }

        //greater
        for (int i = 0; i < DOCS; i++) {
            final Query q1 = select().where(gt("id", from(i)));
            assertEquals(DOCS - i - 1, db.count(q1));
        }

        //greater or equals
        for (int i = 0; i < DOCS; i++) {
            final Query q1 = select().where(gte("id", from(
                    i)));
            assertEquals(DOCS - i, db.count(q1));
        }

        // skip
        final Query qSkip = select().skip(DOCS / 4);
        assertEquals(DOCS - DOCS / 4, db.count(qSkip));

        // limit
        final Query qLimit = select().limit(DOCS / 2);
        assertEquals(DOCS / 2, db.count(qLimit));

        // skip and limit
        final Query qSkipLimit = select().skip(DOCS / 3).limit(DOCS / 2);
        assertEquals(DOCS / 2, db.count(qSkipLimit));
    }

    @Test
    public void filterLong() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            dbBuilder.merge(
                    DatabaseFormat
                            .getCurrent()
                            .newDocumentBuilder()
                            .withField(
                                    "f",
                                    String.format("%07d", i) +
                                    "veryLongFilterableFieldValue",
                                    FILTERABLE)
                            .withPayload(("payload" + i).getBytes())
            );
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Query q =
                select().where(
                        eq("f", from(String.format("%07d", DOCS / 2) + "veryLongFilterableFieldValue")));
        assertEquals(1, db.count(q));
    }

    @Test
    public void sort() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            dbBuilder.merge(
                    DatabaseFormat
                            .getCurrent()
                            .newDocumentBuilder()
                            .withField("id", i, FULL)
                            .withPayload(("payload" + i).getBytes())
            );
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Query q1 =
                select().where(gte("id", from(0)))
                        .and(lte("id", from(DOCS)))
                        .orderBy(desc("id"));
        final List<String> results = new ArrayList<>(DOCS);
        db.execute(q1, new StringProcessor(results));

        // Checking
        int i = DOCS - 1;
        for (String result : results) {
            assertEquals("payload" + i, result);
            i--;
        }
    }

    @Test
    public void stored() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("full", 11, FULL)
                        .withField("sorted", 12, SORTABLE)
                        .withField("stored", 13, STORED)
                        .withField("first", 14, STORED)

                        .withField(LONG_STORED_FIELD_NAME, LONG_FIELD_VALUE, STORED)
                        .withField(LONG_FULL_FIELD_NAME, LONG_FIELD_VALUE, FULL)
                        .withField(LONG_SORTABLE_FIELD_NAME, LONG_FIELD_VALUE, SORTABLE)

                        .withField(INT_STORED_FIELD_NAME, INT_FIELD_VALUE, STORED)
                        .withField(INT_FULL_FIELD_NAME, INT_FIELD_VALUE, FULL)
                        .withField(INT_SORTABLE_FIELD_NAME, INT_FIELD_VALUE, SORTABLE)

                        .withField(SHORT_STORED_FIELD_NAME, SHORT_FIELD_VALUE, STORED)
                        .withField(SHORT_FULL_FIELD_NAME, SHORT_FIELD_VALUE, FULL)
                        .withField(SHORT_SORTABLE_FIELD_NAME, SHORT_FIELD_VALUE, SORTABLE)

                        .withField(CHAR_STORED_FIELD_NAME, CHAR_FIELD_VALUE, STORED)
                        .withField(CHAR_FULL_FIELD_NAME, CHAR_FIELD_VALUE, FULL)
                        .withField(CHAR_SORTABLE_FIELD_NAME, CHAR_FIELD_VALUE, SORTABLE)

                        .withField(BYTE_STORED_FIELD_NAME, BYTE_FIELD_VALUE, STORED)
                        .withField(BYTE_FULL_FIELD_NAME, BYTE_FIELD_VALUE, FULL)
                        .withField(BYTE_SORTABLE_FIELD_NAME, BYTE_FIELD_VALUE, SORTABLE)

                        .withPayload(("payload1").getBytes())
        );

        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("full", 21, FULL)
                        .withField("sorted", 22, SORTABLE)
                        .withField("stored", 23, STORED)
                        .withField("second", 24, STORED)
                        .withPayload(("payload2").getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        // Document 1

        assertEquals(from(11).toByteBuffer(), db.getFieldValue(0, "full"));
        assertEquals(from(12).toByteBuffer(), db.getFieldValue(0, "sorted"));
        assertEquals(from(13).toByteBuffer(), db.getFieldValue(0, "stored"));
        assertEquals(from(14).toByteBuffer(), db.getFieldValue(0, "first"));
        assertEquals(from("payload1").toByteBuffer(), db.getDocument(0));
        assertFalse(db.getFieldValue(0, "second").hasRemaining());

        assertEquals(LONG_FIELD_VALUE, db.getLongValue(0, LONG_STORED_FIELD_NAME));
        assertEquals(LONG_FIELD_VALUE, db.getLongValue(0, LONG_FULL_FIELD_NAME));
        assertEquals(LONG_FIELD_VALUE, db.getLongValue(0, LONG_SORTABLE_FIELD_NAME));

        assertEquals(INT_FIELD_VALUE, db.getIntValue(0, INT_STORED_FIELD_NAME));
        assertEquals(INT_FIELD_VALUE, db.getIntValue(0, INT_FULL_FIELD_NAME));
        assertEquals(INT_FIELD_VALUE, db.getIntValue(0, INT_SORTABLE_FIELD_NAME));

        assertEquals(SHORT_FIELD_VALUE, db.getShortValue(0, SHORT_STORED_FIELD_NAME));
        assertEquals(SHORT_FIELD_VALUE, db.getShortValue(0, SHORT_FULL_FIELD_NAME));
        assertEquals(SHORT_FIELD_VALUE, db.getShortValue(0, SHORT_SORTABLE_FIELD_NAME));

        assertEquals(CHAR_FIELD_VALUE, db.getCharValue(0, CHAR_STORED_FIELD_NAME));
        assertEquals(CHAR_FIELD_VALUE, db.getCharValue(0, CHAR_FULL_FIELD_NAME));
        assertEquals(CHAR_FIELD_VALUE, db.getCharValue(0, CHAR_SORTABLE_FIELD_NAME));

        assertEquals(BYTE_FIELD_VALUE, db.getByteValue(0, BYTE_STORED_FIELD_NAME));
        assertEquals(BYTE_FIELD_VALUE, db.getByteValue(0, BYTE_FULL_FIELD_NAME));
        assertEquals(BYTE_FIELD_VALUE, db.getByteValue(0, BYTE_SORTABLE_FIELD_NAME));

        // Document 2

        assertEquals(from(21).toByteBuffer(), db.getFieldValue(1, "full"));
        assertEquals(from(22).toByteBuffer(), db.getFieldValue(1, "sorted"));
        assertEquals(from(23).toByteBuffer(), db.getFieldValue(1, "stored"));
        assertEquals(from(24).toByteBuffer(), db.getFieldValue(1, "second"));
        assertEquals(from("payload2").toByteBuffer(), db.getDocument(1));
        assertFalse(db.getFieldValue(1, "first").hasRemaining());
    }

    @Test
    public void sortNaturally() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            dbBuilder.merge(
                    DatabaseFormat
                            .getCurrent()
                            .newDocumentBuilder()
                            .withField("f", 0, FULL)
                            .withPayload(("payload" + i).getBytes())
            );
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Query q1 =
                select().where(eq("f", from(0)))
                        .orderBy(desc("f"));
        final List<String> results = new ArrayList<>(DOCS);
        db.execute(q1, new StringProcessor(results));

        // Checking
        int i = 0;
        for (String result : results) {
            assertEquals("payload" + i, result);
            i++;
        }
    }

    @Test
    public void complexSort() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            dbBuilder.merge(
                    DatabaseFormat
                            .getCurrent()
                            .newDocumentBuilder()
                            .withField("f1", i / 2, SORTABLE)
                            .withField("f2", i, SORTABLE)
                            .withPayload(("payload" + i).getBytes())
            );
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Query q =
                select().orderBy(desc("f1")).and(asc("f2"));

        final List<String> results = new ArrayList<>(DOCS);
        db.execute(q, new StringProcessor(results));

        final List<String> expected = new ArrayList<>(DOCS);
        for (int i = DOCS - 2; i >= 0; i -= 2) {
            expected.add("payload" + i);
            expected.add("payload" + (i + 1));
        }

        // Checking
        assertEquals(expected, results);
    }

    @Test
    public void rangeQuery() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            dbBuilder.merge(
                    DatabaseFormat
                            .getCurrent()
                            .newDocumentBuilder()
                            .withField("id", i, FULL)
                            .withPayload(("payload" + i).getBytes())
            );
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Query q1 =
                select().where(
                        new SimpleRangeCondition("id", from(10), true, from(20), true))
                        .orderBy(new SimpleDescendingOrder("id"));
        final List<String> results = new ArrayList<>(DOCS);
        db.execute(q1, new StringProcessor(results));

        // Checking
        int i = 20;
        for (String result : results) {
            assertEquals("payload" + i, result);
            i--;
        }

        final Query q2 =
                select().where(
                        new SimpleRangeCondition("id", from(1000), true, from(2000), true))
                        .orderBy(new SimpleDescendingOrder("id"));
        final List<String> results2 = new ArrayList<>(DOCS);
        db.execute(q2, new StringProcessor(results2));
        assertTrue(results2.isEmpty());
    }

    @Test
    public void buildDatabaseWithBitSetIndex() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc1234", FILTERABLE)
                        .withField("text", "doc123456", FILTERABLE)
                        .withField("int", 1, FILTERABLE)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc2", FILTERABLE)
                        .withField("int", 2, FILTERABLE)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));
        final Query q1 = select().where(eq("int", from(1)));
        assertTrue(db.count(q1) == 1);
        final Query q2 = select().where(eq("int", from(2)));
        assertTrue(db.count(q2) == 1);

        final Query q3 = select().where(eq("int", from(2)))
                .and(in("int", from(2),
                        from(1)));
        assertTrue(db.count(q3) == 1);
    }

    @Test
    public void notQuery() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc1234", FULL)
                        .withField("int", 1, FULL)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc2", FULL)
                        .withField("int", 2, FULL)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Query q1 =
                select().where(not(eq("int", from(1))));
        assertEquals(1, db.count(q1));
        final List<Integer> ids1 = new LinkedList<>();
        db.execute(q1, new DocumentProcessor() {
            @Override
            public boolean process(
                    int document,
                    @NotNull Database database) {
                ids1.add(document);
                return true;
            }
        });
        assertEquals(Collections.singletonList(1), ids1);

        final Query q2 =
                select().where(not(eq("int", from(1))))
                        .and(eq("text", from("doc2")));
        assertEquals(1, db.count(q2));
        final List<Integer> ids2 = new LinkedList<>();
        db.execute(q2, new DocumentProcessor() {
            @Override
            public boolean process(
                    int document,
                    @NotNull Database database) {
                ids2.add(document);
                return true;
            }
        });
        assertEquals(Collections.singletonList(1), ids2);

        final Query q3 =
                select().where(not(eq("int", from(1))))
                        .and(not(eq("int", from(2))));
        assertEquals(0, db.count(q3));
    }

    @Test
    public void orQuery() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc1234", FULL)
                        .withField("int", 1, FULL)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("text", "doc2", FULL)
                        .withField("int", 2, FULL)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Query q =
                select().where(
                        or(
                                eq("int", from(1)),
                                eq("text", from("doc2"))));
        assertEquals(2, db.count(q));
    }

    @Test
    public void empty() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField("id", 0, FULL)
                        .withPayload("payload".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Query query = select().where(eq("id", from(1)));

        assertEquals(0, db.count(query));

        final List<Integer> ids = new LinkedList<>();
        final DocumentProcessor processor = new DocumentProcessor() {
            @Override
            public boolean process(
                    int document,
                    @NotNull Database database) {
                ids.add(document);
                return true;
            }
        };

        db.execute(query, processor);
        assertEquals(0, ids.size());

        assertEquals(0, db.executeAndUnlimitedCount(query, processor));
        assertEquals(0, ids.size());
    }

    @Test
    public void skipAndLimit() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            dbBuilder.merge(
                    DatabaseFormat
                            .getCurrent()
                            .newDocumentBuilder()
                            .withField("id", i, FULL)
                            .withPayload(("payload" + i).getBytes())
            );
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final AtomicInteger ids = new AtomicInteger(DOCS / 4);
        final DocumentProcessor processor = new DocumentProcessor() {
            @Override
            public boolean process(
                    final int document,
                    @NotNull
                    final Database database) {
                assertEquals(
                        ids.getAndIncrement(),
                        document);
                return true;
            }
        };

        // skip
        final Query qSkip = select().skip(DOCS / 4);

        db.execute(qSkip, processor);
        assertEquals(DOCS, ids.get());

        ids.set(DOCS / 4);
        assertEquals(
                DOCS,
                db.executeAndUnlimitedCount(qSkip, processor));
        assertEquals(DOCS, ids.get());

        assertEquals(DOCS - DOCS / 4, db.count(qSkip));

        // limit
        final Query qLimit = select().limit(DOCS / 3);

        ids.set(0);
        db.execute(qLimit, processor);
        assertEquals(DOCS / 3, ids.get());

        ids.set(0);
        assertEquals(
                DOCS,
                db.executeAndUnlimitedCount(qLimit, processor));
        assertEquals(DOCS / 3, ids.get());

        assertEquals(DOCS / 3, db.count(qLimit));

        // skip and limit
        final Query qSkipLimit = select().skip(DOCS / 3).limit(DOCS / 2);

        ids.set(DOCS / 3);
        db.execute(qSkipLimit, processor);
        assertEquals(5 * DOCS / 6, ids.get());

        ids.set(DOCS / 3);
        assertEquals(
                DOCS,
                db.executeAndUnlimitedCount(qSkipLimit, processor));
        assertEquals(5 * DOCS / 6, ids.get());

        assertEquals(DOCS / 2, db.count(qSkipLimit));
    }

    @Test
    public void stopOnFirst() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            dbBuilder.merge(
                    DatabaseFormat
                            .getCurrent()
                            .newDocumentBuilder()
                            .withField("id", i, FULL)
                            .withPayload(("payload" + i).getBytes())
            );
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Collection<Integer> ids = new LinkedList<>();
        final DocumentProcessor processor = new DocumentProcessor() {
            @Override
            public boolean process(
                    final int document,
                    @NotNull
                    final Database database) {
                ids.add(document);
                return false;
            }
        };

        final Query select = select();

        db.execute(select, processor);
        assertEquals(Collections.singletonList(0), ids);

        ids.clear();
        assertEquals(DOCS, db.executeAndUnlimitedCount(select, processor));
        assertEquals(Collections.singletonList(0), ids);
    }
}
