/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.query;

import com.yandex.yoctodb.query.BitSetPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link BoundedBitSetPool}
 *
 * @author incubos
 */
public class BoundedBitSetPoolTest {
    private final int BIT_SET_SIZE = 1;

    @Test
    public void borrowUnderLimit() {
        new BoundedBitSetPool(
                1,
                new ArrayDeque<BitSet>(),
                BIT_SET_SIZE).borrowSet();
    }

    @Test(expected = NoSuchElementException.class)
    public void reachTheLimit() {
        final BitSetPool pool =
                new BoundedBitSetPool(
                        2,
                        new ArrayDeque<BitSet>(),
                        BIT_SET_SIZE);
        pool.borrowSet();
        pool.borrowSet();
    }

    @Test
    public void emptyPop() {
        final BoundedBitSetPool pool =
                new BoundedBitSetPool(
                        2,
                        new ArrayDeque<BitSet>(),
                        BIT_SET_SIZE);
        assertArrayEquals(
                new BitSet[0],
                pool.popUsedBitSets().toArray());
    }

    @Test(expected = IllegalStateException.class)
    public void doubleEmptyPop() {
        final BoundedBitSetPool pool =
                new BoundedBitSetPool(
                        2,
                        new ArrayDeque<BitSet>(),
                        BIT_SET_SIZE);
        pool.popUsedBitSets();
        pool.popUsedBitSets();
    }

    @Test
    public void pop() {
        final BoundedBitSetPool pool =
                new BoundedBitSetPool(
                        2,
                        new ArrayDeque<BitSet>(),
                        BIT_SET_SIZE);
        final BitSet s = pool.borrowSet();
        pool.returnSet(s);
        assertArrayEquals(
                new BitSet[]{s},
                pool.popUsedBitSets().toArray());
    }

    @Test(expected = IllegalStateException.class)
    public void doublePop() {
        final BoundedBitSetPool pool =
                new BoundedBitSetPool(
                        2,
                        new ArrayDeque<BitSet>(),
                        BIT_SET_SIZE);
        final BitSet s = pool.borrowSet();
        pool.returnSet(s);
        pool.popUsedBitSets();
        pool.popUsedBitSets();
    }

    @Test
    public void reuseBitSets() {
        final BitSetPool pool =
                new BoundedBitSetPool(
                        2,
                        new ArrayDeque<BitSet>(),
                        BIT_SET_SIZE);
        final BitSet s = pool.borrowSet();
        pool.returnSet(s);
        assertEquals(s, pool.borrowSet());
    }
}
