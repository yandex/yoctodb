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
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.*;
import static com.yandex.yoctodb.query.QueryBuilder.*;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static com.yandex.yoctodb.util.buf.Buffer.mmap;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Tests for a composite database
 *
 * @author incubos
 */
public class CompositeMmapDatabaseTest {
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

    private static IndexedDatabase db1;
    private static IndexedDatabase db2;
    private static Database db;

    private static File buildDatabase1(final String name) throws IOException {
        final File file = new File(BASE, name);

        final DatabaseBuilder builder = FORMAT.newDatabaseBuilder();

        for (int i = 0; i < DOCS; i++) {
            builder.merge(
                    FORMAT.newDocumentBuilder()
                            .withField("field1", "1", FILTERABLE)
                            .withField("field2", "2", FILTERABLE)
                            .withField("index", i, FULL)
                            .withField("relevance", -i, SORTABLE)
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

    @BeforeClass
    public static void beforeAll() throws IOException {
        db1 = READER.from(mmap(buildDatabase1("1.dat")));
        db2 = READER.from(mmap(buildDatabase2("2.dat")));
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
    public void sortAndLimit() {
        // skip
        final Query qSkip = select().skip(DOCS / 4);
        assertEquals(2 * DOCS - DOCS / 4, db.count(qSkip));

        // limit
        final Query qLimit = select().limit(DOCS / 3);
        assertEquals(DOCS / 3, db.count(qLimit));

        // skip and limit
        final Query qSkipLimit = select().skip(DOCS / 3).limit(DOCS);
        assertEquals(DOCS, db.count(qSkipLimit));
    }

    @Test
    public void filter() {
        for (int i = 0; i < DOCS; i++)
            assertEquals(
                    2,
                    db.count(select().where(eq("index", from(i)))));

        assertEquals(
                DOCS,
                db.count(
                        select()
                                .where(eq("field1", from("1")))
                                .and(eq("field2", from("2")))));
        assertEquals(
                DOCS,
                db.count(
                        select()
                                .where(eq("field1", from("2")))
                                .and(eq("field2", from("1")))));
        assertEquals(
                0,
                db.count(
                        select()
                                .where(eq("field1", from("1")))
                                .and(eq("field2", from("1")))));
        assertEquals(
                0,
                db.count(
                        select()
                                .where(eq("field1", from("2")))
                                .and(eq("field2", from("2")))));
    }

    @Test
    public void sort() {
        final List<Integer> docs = new ArrayList<Integer>(2 * DOCS);
        db.execute(
                select().orderBy(asc("relevance")),
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            @NotNull
                            final Database database) {
                        docs.add(document);
                        return true;
                    }
                }
        );

        assertEquals(2 * DOCS, docs.size());

        final Iterator<Integer> docsIterator = docs.iterator();
        for (int i = DOCS - 1; i >= 0; i--)
            assertEquals(i, docsIterator.next().intValue());
        for (int i = 0; i < DOCS; i++)
            assertEquals(i, docsIterator.next().intValue());
    }

    @Test
    public void sortAndFilter() {
        for (int i = 0; i < DOCS; i++) {
            final List<Integer> docs = new ArrayList<Integer>(2);
            db.execute(
                    select().where(eq("index", from(i)))
                            .orderBy(asc("relevance")),
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
                    }
            );

            assertEquals(2, docs.size());
            assertEquals(Arrays.asList(-i, i), docs);
        }
    }

    @Test
    public void emptyRangeRight() {
        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(
                        in(
                                "relevance",
                                from(DOCS),
                                true,
                                from(2 * DOCS),
                                false)
                )
                        .orderBy(desc("relevance")),
                new DoubleDatabaseProcessor(db1, db2, docs)
        );

        assertEquals(0, docs.size());
    }

    @Test
    public void emptyRangeLeft() {
        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(
                        in(
                                "relevance",
                                from(-2 * DOCS),
                                true,
                                from(-DOCS),
                                false)
                )
                        .orderBy(desc("relevance")),
                new DoubleDatabaseProcessor(db1, db2, docs)
        );

        assertEquals(0, docs.size());
    }

    @Test
    public void unindexedFieldSearch() {
        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(
                        in(
                                "unindexed_field",
                                from(-2 * DOCS),
                                true,
                                from(-DOCS),
                                false)
                )
                        .orderBy(desc("relevance")),
                new DoubleDatabaseProcessor(db1, db2, docs)
        );

        assertEquals(0, docs.size());
    }


    @Test
    public void emptyCompositeDatabaseFieldSearh() {
        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(
                        in(
                                "field1",
                                from(-2 * DOCS),
                                true,
                                from(-DOCS),
                                false)
                )
                        .orderBy(desc("relevance")),
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            @NotNull
                            final Database database) {
                        docs.add(document);
                        return true;
                    }
                }
        );

        assertEquals(0, docs.size());
    }


    @Test
    public void noSortNoLimitSearch() {
        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(gt("index", from(-1))),
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
                }
        );

        assertEquals(DOCS * 2, docs.size());
    }
}
