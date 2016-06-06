/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import com.yandex.yoctodb.DatabaseFormat;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.impl.ReadOnlyOneBitSet;
import com.yandex.yoctodb.v1.immutable.V1Database;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.FULL;
import static com.yandex.yoctodb.query.QueryBuilder.asc;

/**
 * Unit tests for {@link com.yandex.yoctodb.query.simple.SortingScoredDocumentIterator}
 *
 * @author incubos
 */
public class SortingScoredDocumentIteratorTest {
    private final DatabaseFormat FORMAT = DatabaseFormat.getCurrent();

    @Test(expected = UnsupportedOperationException.class)
    public void unsupportedRemove() throws IOException {
        final DatabaseBuilder dbBuilder = FORMAT.newDatabaseBuilder();

        // Document 1
        dbBuilder.merge(
                FORMAT.newDocumentBuilder()
                        .withField("text", "doc1234", FULL)
                        .withField("int", 1, FULL)
                        .withPayload("payload1".getBytes()));

        // Document 2
        dbBuilder.merge(
                FORMAT.newDocumentBuilder()
                        .withField("text", "doc2", FULL)
                        .withField("int", 2, FULL)
                        .withPayload("payload2".getBytes()));

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        final V1Database db = (V1Database) DatabaseFormat.getCurrent()
                .getDatabaseReader()
                .from(Buffer.from(os.toByteArray()));

        final SortingScoredDocumentIterator iterator =
                new SortingScoredDocumentIterator(
                        db,
                        new ReadOnlyOneBitSet(db.getDocumentCount()),
                        Collections.singletonList(asc("text")));

        iterator.remove();
    }
}
