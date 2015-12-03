/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb;

import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.simple.SimpleDescendingOrder;
import com.yandex.yoctodb.query.simple.SimpleRangeCondition;
import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.yandex.yoctodb.query.QueryBuilder.*;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for building and searching a simple database
 *
 * @author incubos
 */
public class SimpleDatabaseTest {
    private final int DOCS = 128;

    private static class StringProcessor implements DocumentProcessor {
        private final List<String> results;

        public StringProcessor(final List<String> results) {
            this.results = results;
        }

        @Override
        public boolean process(
                final int document,
                @NotNull
                final Database database) {
            final Buffer payload = database.getDocument(document);
            final byte[] buf = new byte[(int) payload.remaining()];
            payload.get(buf);
            results.add(new String(buf));
            return true;
        }
    }

    @Test
    public void buildDatabase() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "text",
                                "doc1234",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                1,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "text",
                                "doc2",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                2,
                                DocumentBuilder.IndexOption.FULL)
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
                        .withField(
                                "text",
                                "doc1234",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                1,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "text",
                                "doc2",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                2,
                                DocumentBuilder.IndexOption.FULL)
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
                        .withField(
                                "string_field_full",
                                "string_1",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "string_field_filterable",
                                "string_1",
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "string_field_sortable",
                                "string_1",
                                DocumentBuilder.IndexOption.SORTABLE)
                                //byte
                        .withField(
                                "byte_field_full",
                                (byte) 1,
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "byte_field_filterable",
                                (byte) 1,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "byte_field_sortable",
                                (byte) 1,
                                DocumentBuilder.IndexOption.SORTABLE)
                                //short
                        .withField(
                                "short_field_full",
                                (short) 1,
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "short_field_filterable",
                                (short) 1,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "short_field_sortable",
                                (short) 1,
                                DocumentBuilder.IndexOption.SORTABLE)
                                //integer
                        .withField(
                                "int_field_full",
                                1,
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int_field_filterable",
                                1,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "int_field_sortable",
                                1,
                                DocumentBuilder.IndexOption.SORTABLE)
                                //long
                        .withField(
                                "long_field_full",
                                1L,
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "long_field_filterable",
                                1L,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "long_field_sortable",
                                1L,
                                DocumentBuilder.IndexOption.SORTABLE)
                                //boolean
                        .withField(
                                "boolean_field_full",
                                false,
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "boolean_field_filterable",
                                false,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "boolean_field_sortable",
                                false,
                                DocumentBuilder.IndexOption.SORTABLE)
                                //
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "string_field_full",
                                "doc2",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int_field_full",
                                2,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Collection<Integer> docs = new LinkedList<Integer>();
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
                        .withField(
                                "text",
                                "doc1234",
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "text",
                                "doc1",
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "doc2_not_contain_this_field",
                                "doc2",
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "doc2_not_contain_this_field2",
                                1,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "text",
                                "doc2",
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "int",
                                2,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Collection<Integer> docs = new LinkedList<Integer>();
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
                            .withField(
                                    "id",
                                    i,
                                    DocumentBuilder.IndexOption.FULL)
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
                                    DocumentBuilder.IndexOption.FILTERABLE)
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
                        eq(
                                "f",
                                from(
                                        String.format("%07d", DOCS / 2) +
                                        "veryLongFilterableFieldValue")));
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
                            .withField(
                                    "id",
                                    i,
                                    DocumentBuilder.IndexOption.FULL)
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
        final List<String> results = new ArrayList<String>(DOCS);
        db.execute(q1, new StringProcessor(results));

        // Checking
        int i = DOCS - 1;
        for (String result : results) {
            assertEquals("payload" + i, result);
            i--;
        }
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
                            .withField(
                                    "f",
                                    0,
                                    DocumentBuilder.IndexOption.FULL)
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
        final List<String> results = new ArrayList<String>(DOCS);
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
                            .withField(
                                    "f1",
                                    i / 2,
                                    DocumentBuilder.IndexOption.SORTABLE)
                            .withField(
                                    "f2",
                                    i,
                                    DocumentBuilder.IndexOption.SORTABLE)
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

        final List<String> results = new ArrayList<String>(DOCS);
        db.execute(q, new StringProcessor(results));

        final List<String> expected = new ArrayList<String>(DOCS);
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
                            .withField(
                                    "id",
                                    i,
                                    DocumentBuilder.IndexOption.FULL)
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
                        new SimpleRangeCondition(
                                "id",
                                from(10),
                                true,
                                from(20),
                                true))
                        .orderBy(new SimpleDescendingOrder("id"));
        final List<String> results = new ArrayList<String>(DOCS);
        db.execute(q1, new StringProcessor(results));

        // Checking
        int i = 20;
        for (String result : results) {
            assertEquals("payload" + i, result);
            i--;
        }

        final Query q2 =
                select().where(new SimpleRangeCondition("id",
                        from(1000),
                        true, from(2000),
                        true)).orderBy(new SimpleDescendingOrder("id"));
        final List<String> results2 = new ArrayList<String>(DOCS);
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
                        .withField(
                                "text",
                                "doc1234",
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField("text", "doc123456", DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "int",
                                1,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "text",
                                "doc2",
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withField(
                                "int",
                                2,
                                DocumentBuilder.IndexOption.FILTERABLE)
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
                        .withField(
                                "text",
                                "doc1234",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                1,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "text",
                                "doc2",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                2,
                                DocumentBuilder.IndexOption.FULL)
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
        final List<Integer> ids1 = new LinkedList<Integer>();
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
        final List<Integer> ids2 = new LinkedList<Integer>();
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
                        .withField(
                                "text",
                                "doc1234",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                1,
                                DocumentBuilder.IndexOption.FULL)
                        .withPayload("payload1".getBytes())
        );

        // Document 2
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "text",
                                "doc2",
                                DocumentBuilder.IndexOption.FULL)
                        .withField(
                                "int",
                                2,
                                DocumentBuilder.IndexOption.FULL)
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
                        .withField(
                                "id",
                                0,
                                DocumentBuilder.IndexOption.FULL)
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

        final List<Integer> ids = new LinkedList<Integer>();
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
                            .withField(
                                    "id",
                                    i,
                                    DocumentBuilder.IndexOption.FULL)
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
                            .withField(
                                    "id",
                                    i,
                                    DocumentBuilder.IndexOption.FULL)
                            .withPayload(("payload" + i).getBytes())
            );
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        final Collection<Integer> ids = new LinkedList<Integer>();
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
