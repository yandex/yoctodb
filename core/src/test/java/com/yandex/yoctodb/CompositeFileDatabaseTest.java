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
import com.yandex.yoctodb.immutable.DatabaseReader;
import com.yandex.yoctodb.immutable.IndexedDatabase;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.impl.RejectingArrayBitSetPool;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.*;
import static com.yandex.yoctodb.query.QueryBuilder.*;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Tests for a synchronized composite database
 *
 * @author incubos
 */
public class CompositeFileDatabaseTest {
    private static final String BASE;

    private static final String LONG_STORED_FILED_NAME = "stored_long_value";
    private static final String INT_STORED_FILED_NAME = "stored_int_value";
    private static final String SHORT_STORED_FILED_NAME = "stored_short_value";
    private static final String CHAR_STORED_FILED_NAME = "stored_char_value";
    private static final String BYTE_STORED_FIELD_NAME = "stored_byte_value";

    static {
        try {
            BASE = Files.createTempDirectory("indices").toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final int DOCS = 128;
    private static final DatabaseFormat FORMAT = DatabaseFormat.getCurrent();
    private static final DatabaseReader READER = FORMAT.getDatabaseReader();

    private static IndexedDatabase db1;
    private static IndexedDatabase db2;
    private static Database db;

    private static File buildDatabase1(final String name) throws IOException {
        final File file = new File(BASE, name);

        final DatabaseBuilder builder = FORMAT.newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            builder.merge(
                    FORMAT.newDocumentBuilder()
                            .withField("field1", from("1"), FILTERABLE, DocumentBuilder.IndexType.TRIE)
                            .withField("field2", "2", FILTERABLE)
                            .withField("index", i, FULL)
                            .withField("relevance", -i, SORTABLE)
                            .withField(LONG_STORED_FILED_NAME, Long.valueOf(i), STORED)
                            .withField(INT_STORED_FILED_NAME, i, STORED)
                            .withField(SHORT_STORED_FILED_NAME, (short) i, STORED)
                            .withField(CHAR_STORED_FILED_NAME, (char) i, STORED)
                            .withField(BYTE_STORED_FIELD_NAME, (byte) i, STORED)
                            .withPayload(("payload1=" + i).getBytes())
            );
        }

        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
            builder.buildWritable().writeTo(os);
        }

        return file;
    }

    private static File buildDatabase2(final String name) throws IOException {
        final File file = new File(BASE, name);

        final DatabaseBuilder builder = FORMAT.newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            builder.merge(
                    FORMAT.newDocumentBuilder()
                            .withField("field1", "2", FILTERABLE)
                            .withField("field2", "1", FILTERABLE)
                            .withField("index", i, FULL)
                            .withField("relevance", i, SORTABLE)
                            .withField(LONG_STORED_FILED_NAME, Long.valueOf(i), STORED)
                            .withField(INT_STORED_FILED_NAME, i, STORED)
                            .withField(SHORT_STORED_FILED_NAME, (short) i, STORED)
                            .withField(CHAR_STORED_FILED_NAME, (char) i, STORED)
                            .withField(BYTE_STORED_FIELD_NAME, (byte) i, STORED)
                            .withPayload(("payload2=" + i).getBytes())
            );
        }

        try (OutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
            builder.buildWritable().writeTo(os);
        }

        return file;
    }

    @BeforeClass
    public static void beforeAll() throws IOException {
        db1 = READER.from(
                Buffer.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel()),
                RejectingArrayBitSetPool.INSTANCE,
                true);

        db2 = READER.from(
                Buffer.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel()),
                RejectingArrayBitSetPool.INSTANCE,
                true);

        db = READER.composite(asList(db1, db2));
    }

    @AfterClass
    public static void afterAll() throws IOException {
        for (String name : new File(BASE).list())
            if (!new File(BASE, name).delete()) {
                throw new IOException();
            }

        if (!new File(BASE).delete()) {
            throw new IOException();
        }
    }

    @Test
    public void build() {
        assertEquals(DOCS, db1.getDocumentCount());
        assertEquals(DOCS, db2.getDocumentCount());
        assertEquals(2 * DOCS, db.getDocumentCount());
    }

    @Test
    public void accessDocuments() {
        for (int i = 0; i < DOCS; i++) {
            assertEquals(
                    Buffer.from(("payload1=" + i).getBytes()),
                    db.getDocument(i));
        }
        for (int i = DOCS; i < DOCS << 1; i++) {
            assertEquals(
                    Buffer.from(("payload2=" + (i - DOCS)).getBytes()),
                    db.getDocument(i));
        }
    }

    @Test
    public void skipAndLimit() {
        // skip
        final Query qSkip = select().skip(DOCS / 4);
        final AtomicInteger idSkip = new AtomicInteger(DOCS / 4);
        assertEquals(
                2 * DOCS,
                db.executeAndUnlimitedCount(
                        qSkip,
                        new DocumentProcessor() {
                            @Override
                            public boolean process(
                                    final int document,
                                    @NotNull
                                    final Database database) {
                                assertEquals(
                                        idSkip.getAndIncrement() % DOCS,
                                        document);
                                return true;
                            }
                        }));
        assertEquals(2 * DOCS - DOCS / 4, db.count(qSkip));

        // limit
        final Query qLimit = select().limit(DOCS / 3);
        final AtomicInteger idLimit = new AtomicInteger();
        assertEquals(
                2 * DOCS,
                db.executeAndUnlimitedCount(
                        qLimit,
                        new DocumentProcessor() {
                            @Override
                            public boolean process(
                                    final int document,
                                    @NotNull
                                    final Database database) {
                                assertEquals(
                                        idLimit.getAndIncrement(),
                                        document);
                                return true;
                            }
                        }));
        assertEquals(DOCS / 3, db.count(qLimit));

        // skip and limit
        final Query qSkipLimit = select().skip(DOCS / 3).limit(DOCS);
        final AtomicInteger idSkipLimit = new AtomicInteger(DOCS / 3);
        assertEquals(
                2 * DOCS,
                db.executeAndUnlimitedCount(
                        qSkipLimit,
                        new DocumentProcessor() {
                            @Override
                            public boolean process(
                                    final int document,
                                    @NotNull
                                    final Database database) {
                                assertEquals(
                                        idSkipLimit.getAndIncrement() % DOCS,
                                        document);
                                return true;
                            }
                        }));
        assertEquals(DOCS, db.count(qSkipLimit));
    }

    @Test
    public void filter() {
        for (int i = 0; i < DOCS; i++) {
            final Query q = select().where(eq("index", from(i)));

            final Iterator<IndexedDatabase> dbs =
                    asList(db1, db2).iterator();
            assertEquals(
                    2,
                    db.executeAndUnlimitedCount(
                            q,
                            new DocumentProcessor() {
                                @Override
                                public boolean process(
                                        final int document,
                                        @NotNull
                                        final Database database) {
                                    assertEquals(dbs.next(), database);
                                    return true;
                                }
                            }));

            assertEquals(2, db.count(q));
        }

        final Query qDatabase1Docs =
                select()
                        .where(eq("field1", from("1")))
                        .and(eq("field2", from("2")));
        final AtomicInteger idDatabase1Docs = new AtomicInteger();
        assertEquals(
                DOCS,
                db.executeAndUnlimitedCount(
                        qDatabase1Docs,
                        new DocumentProcessor() {
                            @Override
                            public boolean process(
                                    final int document,
                                    @NotNull
                                    final Database database) {
                                assertEquals(db1, database);
                                assertEquals(
                                        idDatabase1Docs.getAndIncrement(),
                                        document);
                                return true;
                            }
                        }));
        assertEquals(DOCS, db.count(qDatabase1Docs));

        final Query qDatabase2Docs =
                select()
                        .where(eq("field1", from("2")))
                        .and(eq("field2", from("1")));
        final AtomicInteger idDatabase2Docs = new AtomicInteger();
        assertEquals(
                DOCS,
                db.executeAndUnlimitedCount(
                        qDatabase2Docs,
                        new DocumentProcessor() {
                            @Override
                            public boolean process(
                                    final int document,
                                    @NotNull
                                    final Database database) {
                                assertEquals(db2, database);
                                assertEquals(
                                        idDatabase2Docs.getAndIncrement(),
                                        document);
                                return true;
                            }
                        }));
        assertEquals(DOCS, db.count(qDatabase2Docs));

        final Query qZero1 =
                select()
                        .where(eq("field1", from("1")))
                        .and(eq("field2", from("1")));
        assertEquals(
                0,
                db.executeAndUnlimitedCount(
                        qZero1,
                        new DocumentProcessor() {
                            @Override
                            public boolean process(
                                    final int document,
                                    @NotNull
                                    final Database database) {
                                return true;
                            }
                        }));
        assertEquals(0, db.count(qZero1));

        final Query qZero2 =
                select()
                        .where(eq("field1", from("2")))
                        .and(eq("field2", from("2")));
        assertEquals(
                0,
                db.executeAndUnlimitedCount(
                        qZero2,
                        new DocumentProcessor() {
                            @Override
                            public boolean process(
                                    final int document,
                                    @NotNull
                                    final Database database) {
                                return true;
                            }
                        }));
        assertEquals(0, db.count(qZero2));
    }

    @Test
    public void filterDatabase() {
        final Query query =
                select()
                        .where(eq("field1", from("2")))
                        .skip(DOCS / 4)
                        .limit(DOCS / 4);

        final AtomicInteger docs = new AtomicInteger();
        final AtomicInteger expected = new AtomicInteger(DOCS + DOCS / 4);
        final DocumentProcessor processor =
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            @NotNull
                            final Database database) {
                        assertEquals(db2, database);
                        assertEquals(
                                expected.getAndIncrement() - DOCS,
                                document);
                        return docs.incrementAndGet() < DOCS / 4;
                    }
                };

        db.execute(query, processor);
        assertEquals(DOCS / 4, docs.get());

        docs.set(0);
        expected.set(DOCS + DOCS / 4);
        assertEquals(DOCS, db.executeAndUnlimitedCount(query, processor));
    }

    @Test
    public void sort() {
        final Query query =
                select().orderBy(asc("relevance")).and(desc("index"));

        final List<Integer> docs = new ArrayList<>(2 * DOCS);
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

        db.execute(query, processor);
        assertEquals(2 * DOCS, docs.size());

        {
            final Iterator<Integer> docsIterator = docs.iterator();
            for (int i = DOCS - 1; i >= 0; i--)
                assertEquals(i, docsIterator.next().intValue());
            for (int i = 0; i < DOCS; i++)
                assertEquals(i, docsIterator.next().intValue());
        }

        docs.clear();
        assertEquals(2 * DOCS, db.executeAndUnlimitedCount(query, processor));

        {
            final Iterator<Integer> docsIterator = docs.iterator();
            for (int i = DOCS - 1; i >= 0; i--)
                assertEquals(i, docsIterator.next().intValue());
            for (int i = 0; i < DOCS; i++)
                assertEquals(i, docsIterator.next().intValue());
        }
    }

    @Test
    public void sortAndFilter() {
        for (int i = 0; i < DOCS; i++) {
            final Query query =
                    select().where(eq("index", from(i)))
                            .orderBy(asc("relevance"));

            final List<Integer> docs = new ArrayList<>(2);
            final DocumentProcessor processor =
                    new DoubleDatabaseProcessor(db1, db2, docs);

            db.execute(query, processor);

            assertEquals(2, docs.size());
            assertEquals(asList(-i, i), docs);

            docs.clear();

            assertEquals(2, db.executeAndUnlimitedCount(query, processor));
            assertEquals(asList(-i, i), docs);
        }

        for (int i = 0; i < DOCS; i++) {
            final Query query =
                    select().where(eq("index", from(i)))
                            .orderBy(desc("relevance"));
            final List<Integer> docs = new ArrayList<>(2);
            final DocumentProcessor processor =
                    new DoubleDatabaseProcessor(db2, db1, docs);

            db.execute(query, processor);

            assertEquals(2, docs.size());
            assertEquals(asList(-i, i), docs);

            docs.clear();

            assertEquals(2, db.executeAndUnlimitedCount(query, processor));
            assertEquals(asList(-i, i), docs);
        }
    }

    @Test
    public void emptyRangeRight() {
        final Query query =
                select().where(
                        in(
                                "relevance",
                                from(DOCS),
                                true,
                                from(2 * DOCS),
                                false))
                        .orderBy(desc("relevance"));

        final List<Integer> docs = new ArrayList<>();
        final DocumentProcessor processor =
                new DoubleDatabaseProcessor(db1, db2, docs);

        db.execute(query, processor);

        assertEquals(0, docs.size());

        assertEquals(0, db.executeAndUnlimitedCount(query, processor));
        assertEquals(0, docs.size());
    }

    @Test
    public void emptyRangeLeft() {
        final Query query =
                select().where(
                        in(
                                "relevance",
                                from(-2 * DOCS),
                                true,
                                from(-DOCS),
                                false))
                        .orderBy(desc("relevance"));

        final List<Integer> docs = new ArrayList<>();
        final DocumentProcessor processor =
                new DoubleDatabaseProcessor(db1, db2, docs);

        db.execute(query, processor);

        assertEquals(0, docs.size());

        assertEquals(0, db.executeAndUnlimitedCount(query, processor));
        assertEquals(0, docs.size());
    }

    @Test
    public void unindexedFieldSearch() {
        final List<Integer> docs = new ArrayList<>();

        final Query query =
                select().where(
                        in(
                                "unindexed_field",
                                from(-2 * DOCS),
                                true,
                                from(-DOCS),
                                false))
                        .orderBy(desc("relevance"));

        final DocumentProcessor processor =
                new DoubleDatabaseProcessor(db1, db2, docs);

        db.execute(query, processor);

        assertEquals(0, docs.size());

        assertEquals(0, db.executeAndUnlimitedCount(query, processor));
        assertEquals(0, docs.size());
    }

    @Test
    public void emptyCompositeDatabaseFieldSearch() {
        final Database db = READER.composite(new ArrayList<>());

        final Query query =
                select().where(
                        in(
                                "field1",
                                from(-2 * DOCS),
                                true,
                                from(-DOCS),
                                false))
                        .orderBy(desc("relevance"));

        final List<Integer> docs = new ArrayList<>();

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

        db.execute(query, processor);

        assertEquals(0, docs.size());

        assertEquals(0, db.executeAndUnlimitedCount(query, processor));
        assertEquals(0, docs.size());
    }

    @Test
    public void noSortNoLimitSearch() {
        final Query query = select().where(gt("index", from(-1)));

        final List<Integer> docs = new ArrayList<>();
        final DocumentProcessor processor =
                new DoubleDatabaseProcessor(db1, db2, docs);

        db.execute(query, processor);

        assertEquals(DOCS * 2, docs.size());

        docs.clear();

        assertEquals(DOCS * 2, db.executeAndUnlimitedCount(query, processor));
        assertEquals(DOCS * 2, docs.size());
    }

    @Test
    public void extractFieldValues() {
        for (int i = 0; i < 2 * DOCS; i++) {
            final int id = i % DOCS;
            assertEquals(
                    from(id).toByteBuffer(),
                    db.getFieldValue(id, "index"));
            assertEquals(
                    from(-id).toByteBuffer(),
                    db.getFieldValue(id, "relevance"));
        }
    }

    @Test
    public void extractFieldValuesAsLong() {
        for (int i = 0; i < 2 * DOCS; i++) {
            final int id = i % DOCS;
            assertEquals(
                    id,
                    db.getLongValue(id, LONG_STORED_FILED_NAME));
        }
    }

    @Test
    public void extractFieldValueAsInt() {
        for (int i = 0; i < 2 * DOCS; i++) {
            final int id = i % DOCS;
            assertEquals(
                    id,
                    db.getIntValue(id, INT_STORED_FILED_NAME));
        }
    }

    @Test
    public void extractFieldValuesAsShort() {
        for (int i = 0; i < 2 * DOCS; i++) {
            final int id = i % DOCS;
            assertEquals(
                    id,
                    db.getShortValue(id, SHORT_STORED_FILED_NAME));
        }
    }

    @Test
    public void extractFieldValueAsChar() {
        for (int i = 0; i < 2 * DOCS; i++) {
            final int id = i % DOCS;
            assertEquals(
                    id,
                    db.getCharValue(id, CHAR_STORED_FILED_NAME));
        }
    }

    @Test
    public void extractFieldValueAsByte() {
        for (int i = 0; i < 2 * DOCS; i++) {
            final int id = i % DOCS;
            assertEquals(
                    id,
                    db.getByteValue((byte) id, BYTE_STORED_FIELD_NAME));
        }
    }
}
