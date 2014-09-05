/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb;

import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.immutable.DatabaseReader;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.yandex.yoctodb.query.QueryBuilder.*;

/**
 * Tests for a synchronized composite database
 *
 * @author incubos
 */
public class CompositeSynchronizedDatabaseTest {
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
                                "r"));

        Assert.assertEquals(DOCS, db1.getDocumentCount());

        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r"));

        Assert.assertEquals(DOCS, db2.getDocumentCount());

        final Database db = READER.composite(Arrays.asList(db1, db2));

        Assert.assertEquals(2 * DOCS, db.getDocumentCount());
    }

    @Test
    public void sortAndLimit() throws IOException {
        final Database db =
                READER.composite(
                        Arrays.asList(
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase1("1.dat"),
                                                "r")),
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase2("2.dat"),
                                                "r"))));

        // skip
        final Query qSkip = select().skip(DOCS / 4);
        Assert.assertEquals(2 * DOCS - DOCS / 4, db.count(qSkip));

        // limit
        final Query qLimit = select().limit(DOCS / 3);
        Assert.assertEquals(DOCS / 3, db.count(qLimit));

        // skip and limit
        final Query qSkipLimit = select().skip(DOCS / 3).limit(DOCS);
        Assert.assertEquals(DOCS, db.count(qSkipLimit));
    }

    @Test
    public void filter() throws IOException {
        final Database db =
                READER.composite(
                        Arrays.asList(
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase1("1.dat"),
                                                "r")),
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase2("2.dat"),
                                                "r"))));

        for (int i = 0; i < DOCS; i++)
            Assert.assertEquals(
                    2,
                    db.count(select().where(eq("index", UnsignedByteArrays.from(
                                                       i))))
            );

        Assert.assertEquals(
                DOCS,
                db.count(
                        select()
                                .where(eq("field1", UnsignedByteArrays.from(
                                                  "1")))
                                .and(eq("field2", UnsignedByteArrays.from("2")))
                )
        );
        Assert.assertEquals(
                DOCS,
                db.count(
                        select()
                                .where(eq("field1", UnsignedByteArrays.from(
                                                  "2")))
                                .and(eq("field2", UnsignedByteArrays.from("1")))
                )
        );
        Assert.assertEquals(
                0,
                db.count(
                        select()
                                .where(eq("field1", UnsignedByteArrays.from(
                                                  "1")))
                                .and(eq("field2", UnsignedByteArrays.from("1")))
                )
        );
        Assert.assertEquals(
                0,
                db.count(
                        select()
                                .where(eq("field1", UnsignedByteArrays.from(
                                                  "2")))
                                .and(eq("field2", UnsignedByteArrays.from("2")))
                )
        );
    }

    @Test
    public void sort() throws IOException {
        final Database db =
                READER.composite(
                        Arrays.asList(
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase1("1.dat"),
                                                "r")),
                                READER.from(
                                        new RandomAccessFile(
                                                buildDatabase2("2.dat"),
                                                "r"))));

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

        Assert.assertEquals(2 * DOCS, docs.size());

        final Iterator<Integer> docsIterator = docs.iterator();
        for (int i = DOCS - 1; i >= 0; i--)
            Assert.assertEquals(i, docsIterator.next().intValue());
        for (int i = 0; i < DOCS; i++)
            Assert.assertEquals(i, docsIterator.next().intValue());
    }

    @Test
    public void sortAndFilter() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r"));
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r"));

        final Database db = READER.composite(Arrays.asList(db1, db2));

        for (int i = 0; i < DOCS; i++) {
            final List<Integer> docs = new ArrayList<Integer>(2);
            db.execute(
                    select().where(eq("index", UnsignedByteArrays.from(i)))
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

            Assert.assertEquals(2, docs.size());
            Assert.assertEquals(Arrays.asList(-i, i), docs);
        }
    }

    @Test
    public void emptyRangeRight() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r"));
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r"));

        final Database db = READER.composite(Arrays.asList(db1, db2));

        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(
                        in(
                                "relevance",
                                UnsignedByteArrays.from(DOCS),
                                true,
                                UnsignedByteArrays.from(2 * DOCS),
                                false)
                )
                        .orderBy(desc("relevance")),
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

        Assert.assertEquals(0, docs.size());
    }

    @Test
    public void emptyRangeLeft() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r"));
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r"));

        final Database db = READER.composite(Arrays.asList(db1, db2));

        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(
                        in(
                                "relevance",
                                UnsignedByteArrays.from(-2 * DOCS),
                                true,
                                UnsignedByteArrays.from(-DOCS),
                                false)
                )
                        .orderBy(desc("relevance")),
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

        Assert.assertEquals(0, docs.size());
    }

    @Test
    public void unindexedFieldSearh() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r"));
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r"));

        final Database db = READER.composite(Arrays.asList(db1, db2));

        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(
                        in(
                                "unindexed_field",
                                UnsignedByteArrays.from(-2 * DOCS),
                                true,
                                UnsignedByteArrays.from(-DOCS),
                                false)
                )
                        .orderBy(desc("relevance")),
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

        Assert.assertEquals(0, docs.size());
    }


    @Test
    public void emptyCompositeDatabaseFieldSearh() throws IOException {
        final Database db = READER.composite(new ArrayList<Database>());

        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(
                        in(
                                "field1",
                                UnsignedByteArrays.from(-2 * DOCS),
                                true,
                                UnsignedByteArrays.from(-DOCS),
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

        Assert.assertEquals(0, docs.size());
    }


    @Test
    public void noSortNoLimitSearch() throws IOException {
        final Database db1 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase1("1.dat"),
                                "r"));
        final Database db2 =
                READER.from(
                        new RandomAccessFile(
                                buildDatabase2("2.dat"),
                                "r"));

        final Database db = READER.composite(Arrays.asList(db1, db2));

        final List<Integer> docs = new ArrayList<Integer>();

        db.execute(
                select().where(gt("index", UnsignedByteArrays.from(-1))),
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

        Assert.assertEquals(DOCS * 2, docs.size());
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
