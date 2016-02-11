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
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import static com.yandex.yoctodb.query.QueryBuilder.*;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for partial documents
 *
 * @author incubos
 */
public class PartialDocumentTest {
    @Test
    public void findDocumentsWithFieldValue() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "a",
                                1,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withPayload("doc1".getBytes())
        );

        // Document 1
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withField(
                                "b",
                                2,
                                DocumentBuilder.IndexOption.FILTERABLE)
                        .withPayload("doc2".getBytes())
        );

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        // First document
        final Query q1 = select().where(eq("a", from(1)));
        final List<String> docs1 = new LinkedList<String>();
        db.execute(q1, new StringProcessor(docs1));
        assertEquals(singletonList("doc1"), docs1);

        // Second documents
        final Query q2 = select().where(eq("b", from(2)));
        final List<String> docs2 = new LinkedList<String>();
        db.execute(q2, new StringProcessor(docs2));
        assertEquals(singletonList("doc2"), docs2);

        // No documents
        final Query q3 = select().where(eq("a", from(1))).and(eq("b", from(2)));
        assertEquals(0, db.count(q3));

        // Both documents
        final Query q4 = select().where(
                or(
                        eq("a", from(1)),
                        eq("b", from(2))));
        final List<String> docs4 = new LinkedList<String>();
        db.execute(q4, new StringProcessor(docs4));
        assertEquals(asList("doc1", "doc2"), docs4);
    }

    @Test
    public void documentWithoutFields() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withPayload("doc".getBytes()));

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        assertEquals(1, db.getDocumentCount());

        // None documents
        final Query q1 = select().where(eq("a", from(1)));
        assertEquals(0, db.count(q1));

        // The document
        final Query q2 = select();
        final List<String> docs2 = new LinkedList<String>();
        db.execute(q2, new StringProcessor(docs2));
        assertEquals(singletonList("doc"), docs2);
    }

    @Test
    public void emptyDatabase() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        assertEquals(0, db.getDocumentCount());

        // None documents
        final Query q1 = select();
        assertEquals(0, db.count(q1));
    }

    @Test(expected = NoSuchElementException.class)
    public void emptyDocument() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder());

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        assertEquals(1, db.getDocumentCount());

        // One document
        final Query q1 = select();
        assertEquals(1, db.count(q1));

        db.getDocument(0);
    }

    @Test
    public void emptyPayload() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Document
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withPayload(new byte[0]));

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
        final Database db =
                DatabaseFormat.getCurrent()
                        .getDatabaseReader()
                        .from(Buffer.from(os.toByteArray()));

        assertEquals(1, db.getDocumentCount());

        // One document
        final Query q1 = select();
        assertEquals(1, db.count(q1));

        assertEquals(0, db.getDocument(0).remaining());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void sparseUnsupported() throws IOException {
        final DatabaseBuilder dbBuilder =
                DatabaseFormat.getCurrent().newDatabaseBuilder();

        // Empty document
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder());

        // Nonempty document
        dbBuilder.merge(
                DatabaseFormat
                        .getCurrent()
                        .newDocumentBuilder()
                        .withPayload("doc".getBytes()));

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);
    }
}
