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

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.simple.SimpleDescendingOrder;
import com.yandex.yoctodb.query.simple.SimpleRangeCondition;
import com.yandex.yoctodb.util.UnsignedByteArrays;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.yandex.yoctodb.query.QueryBuilder.*;

/**
 * Tests for building and searching a simple database
 *
 * @author incubos
 */
public class SimpleDatabaseTest {

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
        final Query q1 = select().where(eq("int", UnsignedByteArrays.from(1)));
        Assert.assertTrue(db.count(q1) == 1);
        final Query q2 = select().where(eq("int", UnsignedByteArrays.from(2)));
        Assert.assertTrue(db.count(q2) == 1);

        final Query q3 = select().where(eq("int", UnsignedByteArrays.from(2)))
                .and(in("int", UnsignedByteArrays.from(2), UnsignedByteArrays
                                .from(1)));
        Assert.assertTrue(db.count(q3) == 1);
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
        final Query q1 = select().where(eq("int_field_full", UnsignedByteArrays
                                                   .from(1)));
        Assert.assertTrue(db.count(q1) == 1);
        final Query q2 = select().where(eq("string_field_full", UnsignedByteArrays
                                                   .from("doc2")));
        Assert.assertTrue(db.count(q2) == 1);

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
        final Query q2 = select().where(eq("int", UnsignedByteArrays.from(2)));
        Assert.assertTrue(db.count(q2) == 1);

        final Query q3 = select().where(eq("int", UnsignedByteArrays.from(2)))
                .and(
                        in(
                                "int",
                                UnsignedByteArrays.from(2),
                                UnsignedByteArrays.from(1)));
        Assert.assertTrue(db.count(q3) == 1);
    }

    @Test
    public void countTest() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();
        final int docs = 128;

        for (int i = 0; i < docs; i++) {
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
        for (int i = 0; i < docs; i++) {
            final Query q1 = select().where(lt("id", UnsignedByteArrays.from(i)));
            Assert.assertEquals(i, db.count(q1));
        }

        //less or equals
        for (int i = 0; i < docs; i++) {
            final Query q1 = select().where(lte("id", UnsignedByteArrays.from(
                                                        i)));
            Assert.assertEquals(i + 1, db.count(q1));
        }

        //greater
        for (int i = 0; i < docs; i++) {
            final Query q1 = select().where(gt("id", UnsignedByteArrays.from(i)));
            Assert.assertEquals(docs - i - 1, db.count(q1));
        }

        //greater or equals
        for (int i = 0; i < docs; i++) {
            final Query q1 = select().where(gte("id", UnsignedByteArrays.from(
                                                        i)));
            Assert.assertEquals(docs - i, db.count(q1));
        }

        // skip
        final Query qSkip = select().skip(docs / 4);
        Assert.assertEquals(docs - docs / 4, db.count(qSkip));

        // limit
        final Query qLimit = select().limit(docs / 2);
        Assert.assertEquals(docs / 2, db.count(qLimit));

        // skip and limit
        final Query qSkipLimit = select().skip(docs / 3).limit(docs / 2);
        Assert.assertEquals(docs / 2, db.count(qSkipLimit));
    }

    @Test
    public void sortTest() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();
        final int docs = 128;

        for (int i = 0; i < docs; i++) {
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
                select().where(gte("id", UnsignedByteArrays.from(0)))
                        .and(lte("id", UnsignedByteArrays.from(docs)))
                        .orderBy(desc("id"));
        final List<String> results = new ArrayList<String>(docs);
        db.execute(q1, new DocumentProcessor() {
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
        });

        // Checking
        int i = docs - 1;
        for (String result : results) {
            Assert.assertEquals("payload" + i, result);
            i--;
        }
    }

    @Test
    public void rangeQueryTest() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();
        final int docs = 128;

        for (int i = 0; i < docs; i++) {
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
                                UnsignedByteArrays.from(10),
                                true, UnsignedByteArrays.from(20),
                                true)).orderBy(new SimpleDescendingOrder("id"));
        final List<String> results = new ArrayList<String>(docs);
        db.execute(q1, new DocumentProcessor() {
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
        });

        // Checking
        int i = 20;
        for (String result : results) {
            Assert.assertEquals("payload" + i, result);
            i--;
        }

        final Query q2 =
                select().where(new SimpleRangeCondition("id",
                        UnsignedByteArrays.from(1000),
                        true, UnsignedByteArrays.from(2000),
                        true)).orderBy(new SimpleDescendingOrder("id"));
        final List<String> results2 = new ArrayList<String>(docs);
        db.execute(q2, new DocumentProcessor() {
            @Override
            public boolean process(
                    final int document,
                    @NotNull
                    final Database database) {
                final Buffer payload = database.getDocument(document);
                final byte[] buf = new byte[(int) payload.remaining()];
                payload.get(buf);
                results2.add(new String(buf));
                return true;
            }
        });
        Assert.assertTrue(results2.isEmpty());
    }


    @Test
    public void buildDatabaseWithTrieIndex() throws IOException {
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
                                DocumentBuilder.IndexOption.FILTERABLE_TRIE_BASED)
                        .withField("text", "doc123456", DocumentBuilder.IndexOption.FILTERABLE_TRIE_BASED)
                        .withField(
                                "int",
                                1,
                                DocumentBuilder.IndexOption.FILTERABLE_TRIE_BASED)
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
                                DocumentBuilder.IndexOption.FILTERABLE_TRIE_BASED)
                        .withField(
                                "int",
                                2,
                                DocumentBuilder.IndexOption.FILTERABLE_TRIE_BASED)
                        .withPayload("payload2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));
        final Query q1 = select().where(eq("int", UnsignedByteArrays.from(1)));
        Assert.assertTrue(db.count(q1) == 1);
        final Query q2 = select().where(eq("int", UnsignedByteArrays.from(2)));
        Assert.assertTrue(db.count(q2) == 1);

        final Query q3 = select().where(eq("int", UnsignedByteArrays.from(2)))
                .and(in("int", UnsignedByteArrays.from(2), UnsignedByteArrays
                                .from(1)));
        Assert.assertTrue(db.count(q3) == 1);
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
        final Query q1 = select().where(eq("int", UnsignedByteArrays.from(1)));
        Assert.assertTrue(db.count(q1) == 1);
        final Query q2 = select().where(eq("int", UnsignedByteArrays.from(2)));
        Assert.assertTrue(db.count(q2) == 1);

        final Query q3 = select().where(eq("int", UnsignedByteArrays.from(2)))
                .and(in("int", UnsignedByteArrays.from(2), UnsignedByteArrays
                                .from(1)));
        Assert.assertTrue(db.count(q3) == 1);
    }
}


