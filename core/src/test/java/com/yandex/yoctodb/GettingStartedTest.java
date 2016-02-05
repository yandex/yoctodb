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
import com.yandex.yoctodb.util.buf.Buffer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.FILTERABLE;
import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.SORTABLE;
import static com.yandex.yoctodb.query.QueryBuilder.*;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Getting started guide
 *
 * @author incubos
 */
public class GettingStartedTest {
    @Test
    public void test() throws IOException {
        // Get current database format
        final DatabaseFormat format = DatabaseFormat.getCurrent();

        // Create a mutable database
        final DatabaseBuilder dbBuilder = format.newDatabaseBuilder();

        // Add some document
        dbBuilder.merge(
                format.newDocumentBuilder()
                        .withField("id", 1, FILTERABLE)
                        .withField("score", 0, SORTABLE)
                        .withPayload("payload1".getBytes()));

        // Add another document
        dbBuilder.merge(
                format.newDocumentBuilder()
                        .withField("id", 2, FILTERABLE)
                        .withField("score", 1, SORTABLE)
                        .withPayload("payload2".getBytes()));

        // Build and serialize immutable database
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        // Open the immutable database
        final Database db =
                format.getDatabaseReader().from(
                        Buffer.from(
                                os.toByteArray()));

        // Filter the second document
        final Query doc2 = select().where(eq("id", from(2)));
        assertTrue(db.count(doc2) == 1);

        // Filter and sort

        final Query sorted =
                select().where(and(gte("id", from(1)), lte("id", from(2))))
                        .orderBy(desc("score"));

        final List<Integer> ids = new LinkedList<Integer>();
        db.execute(
                sorted,
                new DocumentProcessor() {
                    @Override
                    public boolean process(
                            final int document,
                            final Database database) {
                        ids.add(document);
                        return true;
                    }
                });

        assertEquals(asList(1, 0), ids);
    }
}
