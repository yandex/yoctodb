/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query;

import com.yandex.yoctodb.util.UnsignedByteArrays;
import org.junit.Test;

import static com.yandex.yoctodb.query.QueryBuilder.*;
import static com.yandex.yoctodb.util.UnsignedByteArrays.from;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link Query}
 *
 * @author incubos
 */
public class QueryBuilderTest {

    @Test
    public void pointRange() {
        in("test", from(1), true, from(1), true);
    }

    @Test
    public void sqlLike() {
        final Query stmt =
                select().where(
                        eq("key1", from("value1"))).and(
                        eq("key2", from("value2"))).orderBy(
                        asc("timestamp")).and(desc("id"))
                        .skip(1)
                        .limit(2);

        assertTrue(stmt.toString().contains("key1"));
        assertTrue(stmt.toString().contains("key2"));
        assertTrue(stmt.toString().contains("timestamp"));
        assertTrue(stmt.toString().contains("id"));
    }

    @Test
    public void mixedWhere() {
        final Query stmt =
                select().where(eq("key1", from("value1")))
                        .where(eq("key2", from("value2")))
                        .orderBy(asc("timestamp"))
                        .where(eq("key3", from("value3")))
                        .skip(1)
                        .where(eq("key4", from("value4")))
                        .limit(2)
                        .where(eq("key5", from("value5")));

        assertTrue(stmt.toString().contains("key1"));
        assertTrue(stmt.toString().contains("key2"));
        assertTrue(stmt.toString().contains("timestamp"));
        assertTrue(stmt.toString().contains("key3"));
        assertTrue(stmt.toString().contains("key4"));
        assertTrue(stmt.toString().contains("key5"));
    }

    @Test
    public void mixedOrderBy() {
        final Query stmt =
                select().where(
                        eq("key1", from("value1"))).and(
                        eq("key2", from("value2")))
                        .orderBy(asc("timestamp"))
                        .orderBy(desc("id"))
                        .skip(1)
                        .orderBy(asc("age"))
                        .limit(2)
                        .orderBy(desc("rnd"));

        assertTrue(stmt.toString().contains("key1"));
        assertTrue(stmt.toString().contains("key2"));
        assertTrue(stmt.toString().contains("timestamp"));
        assertTrue(stmt.toString().contains("id"));
        assertTrue(stmt.toString().contains("age"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyFieldName() {
        in("", from(1), true, from(1), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyValues() {
        in("f");
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyValue() {
        in("f", from(1), UnsignedByteArrays.raw(new byte[0]), from(3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyFrom() {
        in("test", UnsignedByteArrays.raw(new byte[0]), true, from(2), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyTo() {
        in("test", from(1), true, UnsignedByteArrays.raw(new byte[0]), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyLeftRange() {
        in("test", from(1), true, from(1), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyRightRange() {
        in("test", from(1), true, from(1), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void reversedRange() {
        in("test", from(2), true, from(1), false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyLteValue() {
        lte("f", UnsignedByteArrays.raw(new byte[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyLtValue() {
        lt("f", UnsignedByteArrays.raw(new byte[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyGteValue() {
        gte("f", UnsignedByteArrays.raw(new byte[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyGtValue() {
        gt("f", UnsignedByteArrays.raw(new byte[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyEqValue() {
        eq("f", UnsignedByteArrays.raw(new byte[0]));
    }
}
