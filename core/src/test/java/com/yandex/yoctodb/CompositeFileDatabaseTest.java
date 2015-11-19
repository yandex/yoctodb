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
import com.yandex.yoctodb.immutable.DatabaseReader;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.yandex.yoctodb.query.QueryBuilder.*;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertEquals;

/**
 * Tests for a synchronized composite database
 *
 * @author incubos
 */
public class CompositeFileDatabaseTest {
    private static final String BASE;

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

    @Test
    public void build() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel());

        assertEquals(DOCS, db1.getDocumentCount());

        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel());

        assertEquals(DOCS, db2.getDocumentCount());

        final Database db = READER.composite(Arrays.asList(db1, db2));

        assertEquals(2 * DOCS, db.getDocumentCount());
    }

    @Test
    public void accessDocuments() throws IOException {
        final Database db =
                READER.composite(
                        Arrays.asList(
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase1("1.dat"),
                                                "r").getChannel()),
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase2("2.dat"),
                                                "r").getChannel())));
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
    public void skipAndLimit() throws IOException {
        final Database db =
                READER.composite(
                        Arrays.asList(
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase1("1.dat"),
                                                "r").getChannel()),
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase2("2.dat"),
                                                "r").getChannel())));

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
    public void filter() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel());
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel());

        final Database db = READER.composite(Arrays.asList(db1, db2));

        for (int i = 0; i < DOCS; i++) {
            final Query q = select().where(eq("index", from(i)));

            final Iterator<Database> dbs = Arrays.asList(db1, db2).iterator();
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
    public void filterDatabase() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel());
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel());
        final Database db = READER.composite(Arrays.asList(db1, db2));

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
                            @NotNull final Database database) {
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
    public void sort() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel());
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel());
        final Database db = READER.composite(Arrays.asList(db1, db2));

        final Query query =
                select().orderBy(asc("relevance")).and(desc("index"));

        final List<Integer> docs = new ArrayList<Integer>(2 * DOCS);
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
    public void sortAndFilter() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel());
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel());

        final Database db = READER.composite(Arrays.asList(db1, db2));

        for (int i = 0; i < DOCS; i++) {
            final Query query =
                    select().where(eq("index", from(i)))
                            .orderBy(asc("relevance"));

            final List<Integer> docs = new ArrayList<Integer>(2);
            final DocumentProcessor processor =
                    new DocumentProcessor() {
                        @Override
                        public boolean process(
                                final int document,
                                @NotNull
                                final Database database) {
                            if (database == db1) {
                                docs.add(-document);
                            } else {
                                docs.add(document);
                            }
                            return true;
                        }
                    };

            db.execute(query, processor);

            assertEquals(2, docs.size());
            assertEquals(Arrays.asList(-i, i), docs);

            docs.clear();

            assertEquals(2, db.executeAndUnlimitedCount(query, processor));
            assertEquals(Arrays.asList(-i, i), docs);
        }

        for (int i = 0; i < DOCS; i++) {
            final Query query =
                    select().where(eq("index", from(i)))
                            .orderBy(desc("relevance"));
            final List<Integer> docs = new ArrayList<Integer>(2);
            final DocumentProcessor processor =
                    new DocumentProcessor() {
                        @Override
                        public boolean process(
                                final int document,
                                @NotNull
                                final Database database) {
                            if (database == db1) {
                                docs.add(document);
                            } else {
                                docs.add(-document);
                            }
                            return true;
                        }
                    };

            db.execute(query, processor);

            assertEquals(2, docs.size());
            assertEquals(Arrays.asList(-i, i), docs);

            docs.clear();

            assertEquals(2, db.executeAndUnlimitedCount(query, processor));
            assertEquals(Arrays.asList(-i, i), docs);
        }
    }

    @Test
    public void emptyRangeRight() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel());
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel());

        final Database db = READER.composite(Arrays.asList(db1, db2));

        final Query query =
                select().where(
                        in(
                                "relevance",
                                from(DOCS),
                                true,
                                from(2 * DOCS),
                                false))
                        .orderBy(desc("relevance"));

        final List<Integer> docs = new ArrayList<Integer>();
        final DocumentProcessor processor =
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            @NotNull
                            final Database database) {
                        if (database == db1) {
                            docs.add(-document);
                        } else {
                            docs.add(document);
                        }
                        return true;
                    }
                };

        db.execute(query, processor);

        assertEquals(0, docs.size());

        assertEquals(0, db.executeAndUnlimitedCount(query, processor));
        assertEquals(0, docs.size());
    }

    @Test
    public void emptyRangeLeft() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel());
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel());

        final Database db = READER.composite(Arrays.asList(db1, db2));

        final Query query =
                select().where(
                        in(
                                "relevance",
                                from(-2 * DOCS),
                                true,
                                from(-DOCS),
                                false))
                        .orderBy(desc("relevance"));

        final List<Integer> docs = new ArrayList<Integer>();
        final DocumentProcessor processor =
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            @NotNull
                            final Database database) {
                        if (database == db1) {
                            docs.add(-document);
                        } else {
                            docs.add(document);
                        }
                        return true;
                    }
                };

        db.execute(query, processor);

        assertEquals(0, docs.size());

        assertEquals(0, db.executeAndUnlimitedCount(query, processor));
        assertEquals(0, docs.size());
    }

    @Test
    public void unindexedFieldSearch() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel());
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel());

        final Database db = READER.composite(Arrays.asList(db1, db2));

        final List<Integer> docs = new ArrayList<Integer>();

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
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            @NotNull
                            final Database database) {
                        if (database == db1) {
                            docs.add(-document);
                        } else {
                            docs.add(document);
                        }
                        return true;
                    }
                };

        db.execute(query, processor);

        assertEquals(0, docs.size());

        assertEquals(0, db.executeAndUnlimitedCount(query, processor));
        assertEquals(0, docs.size());
    }

    @Test
    public void emptyCompositeDatabaseFieldSearch() throws IOException {
        final Database db = READER.composite(new ArrayList<Database>());

        final Query query =
                select().where(
                        in(
                                "field1",
                                from(-2 * DOCS),
                                true,
                                from(-DOCS),
                                false))
                        .orderBy(desc("relevance"));

        final List<Integer> docs = new ArrayList<Integer>();

        final DocumentProcessor processor =
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            final @NotNull Database database) {
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
    public void noSortNoLimitSearch() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r").getChannel());
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r").getChannel());

        final Database db = READER.composite(Arrays.asList(db1, db2));

        final Query query = select().where(gt("index", from(-1)));

        final List<Integer> docs = new ArrayList<Integer>();

        final DocumentProcessor processor =
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            final @NotNull Database database) {
                        if (database == db1) {
                            docs.add(-document);
                        } else {
                            docs.add(document);
                        }
                        return true;
                    }
                };

        db.execute(query, processor);

        assertEquals(DOCS * 2, docs.size());

        docs.clear();

        assertEquals(DOCS * 2, db.executeAndUnlimitedCount(query, processor));
        assertEquals(DOCS * 2, docs.size());
    }

    private File buildDatabase1(final String name) throws IOException {
        final File file = new File(BASE, name);

        final DatabaseBuilder builder = FORMAT.newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            builder.merge(
                    FORMAT.newDocumentBuilder()
                            .withField(
                                    "field1",
                                    "1",
                                    DocumentBuilder.IndexOption.FILTERABLE)
                            .withField(
                                    "field2",
                                    "2",
                                    DocumentBuilder.IndexOption.FILTERABLE)
                            .withField(
                                    "index",
                                    i,
                                    DocumentBuilder.IndexOption.FULL)
                            .withField(
                                    "relevance",
                                    -i,
                                    DocumentBuilder.IndexOption.SORTABLE)
                            .withPayload(("payload1=" + i).getBytes())
            );
        }

        final OutputStream os =
                new BufferedOutputStream(new FileOutputStream(file));
        try {
            builder.buildWritable().writeTo(os);
        } finally {
            os.close();
        }

        return file;
    }

    private File buildDatabase2(final String name) throws IOException {
        final File file = new File(BASE, name);

        final DatabaseBuilder builder = FORMAT.newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            builder.merge(
                    FORMAT.newDocumentBuilder()
                            .withField(
                                    "field1",
                                    "2",
                                    DocumentBuilder.IndexOption.FILTERABLE)
                            .withField(
                                    "field2",
                                    "1",
                                    DocumentBuilder.IndexOption.FILTERABLE)
                            .withField(
                                    "index",
                                    i,
                                    DocumentBuilder.IndexOption.FULL)
                            .withField(
                                    "relevance",
                                    i,
                                    DocumentBuilder.IndexOption.SORTABLE)
                            .withPayload(("payload2=" + i).getBytes())
            );
        }

        final OutputStream os =
                new BufferedOutputStream(new FileOutputStream(file));
        try {
            builder.buildWritable().writeTo(os);
        } finally {
            os.close();
        }

        return file;
    }

    @After
    public void afterEach() throws IOException {
        for (String name : new File(BASE).list())
            if (!new File(BASE, name).delete()) {
                throw new IOException();
            }
    }

    @AfterClass
    public static void afterAll() throws IOException {
        if (!new File(BASE).delete()) {
            throw new IOException();
        }
    }
}
