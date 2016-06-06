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
import com.yandex.yoctodb.query.Condition;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import com.yandex.yoctodb.query.simple.SimpleAndCondition;
import com.yandex.yoctodb.query.simple.SimpleOrCondition;
import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.FILTERABLE;
import static com.yandex.yoctodb.mutable.DocumentBuilder.IndexOption.FULL;
import static com.yandex.yoctodb.query.QueryBuilder.*;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 * Checking De Morgan laws using query language
 *
 * @author incubos
 */
public class DeMorganQueryTest {
    private DocumentBuilder buildDocument(int x, int y, int z) {
        return DatabaseFormat.getCurrent().newDocumentBuilder()
                .withField("id", x * 4 + y * 2 + z, FULL)
                .withField("x", x, FILTERABLE)
                .withField("y", y, FILTERABLE)
                .withField("z", z, FILTERABLE)
                .withPayload(String.format("%d%d%d", x, y, z).getBytes());
    }

    private Database buildDatabase() throws IOException {
        final DatabaseFormat databaseFormat = DatabaseFormat.getCurrent();
        final DatabaseBuilder dbBuilder = databaseFormat.newDatabaseBuilder();

        dbBuilder.merge(buildDocument(0, 0, 0));
        dbBuilder.merge(buildDocument(0, 0, 1));
        dbBuilder.merge(buildDocument(0, 1, 0));
        dbBuilder.merge(buildDocument(0, 1, 1));
        dbBuilder.merge(buildDocument(1, 0, 0));
        dbBuilder.merge(buildDocument(1, 0, 1));
        dbBuilder.merge(buildDocument(1, 1, 0));
        dbBuilder.merge(buildDocument(1, 1, 1));

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        dbBuilder.buildWritable().writeTo(os);

        return databaseFormat.getDatabaseReader().from(
                Buffer.from(
                        os.toByteArray()));
    }

    private static class IdCollector implements DocumentProcessor {
        @NotNull
        private final List<Integer> ids;

        IdCollector(
                @NotNull
                final List<Integer> ids) {
            this.ids = ids;
        }

        @Override
        public boolean process(
                final int document,
                @NotNull
                final Database database) {
            ids.add(document);
            return true;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalOr() {
        new SimpleOrCondition(Collections.<Condition>emptyList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalAnd() {
        new SimpleAndCondition(Collections.<Condition>emptyList());
    }

    @Test
    public void all() throws IOException {
        final Database db = buildDatabase();
        final Query query = select();

        assertEquals(db.getDocumentCount(), db.count(query));
    }

    @Test
    public void notNonExisting() throws IOException {
        final Database db = buildDatabase();
        final Query query = select().where(not(eq("x", from(2))));

        assertEquals(db.getDocumentCount(), db.count(query));
    }

    @Test
    public void each() throws IOException {
        final Database db = buildDatabase();

        for (int x = 0; x < 2; x++) {
            for (int y = 0; y < 2; y++) {
                for (int z = 0; z < 2; z++) {
                    final Query query =
                            select().where(
                                    and(
                                            eq("x", from(x)),
                                            eq("y", from(y)),
                                            eq("z", from(z))));

                    assertEquals(1, db.count(query));

                    final List<Integer> ids = new LinkedList<>();

                    final int expectedId = x * 4 + y * 2 + z;

                    db.execute(query, new IdCollector(ids));
                    assertEquals(singletonList(expectedId), ids);

                    ids.clear();
                    assertEquals(
                            1,
                            db.executeAndUnlimitedCount(
                                    query,
                                    new IdCollector(ids)));
                    assertEquals(singletonList(expectedId), ids);
                }
            }
        }
    }

    @Test
    public void notOne() throws IOException {
        final Database db = buildDatabase();

        for (int x = 0; x < 2; x++) {
            for (int y = 0; y < 2; y++) {
                for (int z = 0; z < 2; z++) {
                    final Query query =
                            select().where(
                                    not(
                                            and(
                                                    eq("x", from(x)),
                                                    eq("y", from(y)),
                                                    eq("z", from(z)))));

                    assertEquals(7, db.count(query));

                    final List<Integer> actual = new LinkedList<>();
                    final List<Integer> expected = new LinkedList<>();

                    final int unexpected = x * 4 + y * 2 + z;
                    for (int i = 0; i < 8; i++) {
                        if (i != unexpected)
                            expected.add(i);
                    }

                    db.execute(query, new IdCollector(actual));
                    assertEquals(expected, actual);

                    actual.clear();
                    assertEquals(
                            7,
                            db.executeAndUnlimitedCount(
                                    query,
                                    new IdCollector(actual)));
                    assertEquals(expected, actual);
                }
            }
        }
    }

    @Test
    public void allUsingOr() throws IOException {
        final Database db = buildDatabase();

        final Collection<Condition> ors = new LinkedList<>();

        for (int x = 0; x < 2; x++) {
            for (int y = 0; y < 2; y++) {
                for (int z = 0; z < 2; z++) {
                    ors.add(
                            and(
                            eq("x", from(x)),
                            eq("y", from(y)),
                            eq("z", from(z))));
                }
            }
        }

        final Query query = select().where(new SimpleOrCondition(ors));
        assertEquals(db.getDocumentCount(), db.count(query));
    }
}
