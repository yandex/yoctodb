/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for any {@link ArrayBitSetPool} implementation
 *
 * @author incubos
 */
public abstract class AbstractArrayBitSetPoolTestBench {
    protected abstract ArrayBitSetPool allocate();

    @Test
    public void borrowOne() {
        final ArrayBitSetPool pool = allocate();
        final ArrayBitSet set = pool.borrowSet(1);
        assertEquals(1, set.getSize());
        assertEquals(0, set.cardinality());
    }

    @Test
    public void borrowTwo() {
        final ArrayBitSetPool pool = allocate();

        final ArrayBitSet set1 = pool.borrowSet(1);
        assertEquals(1, set1.getSize());
        assertEquals(0, set1.cardinality());

        final ArrayBitSet set2 = pool.borrowSet(1);
        assertEquals(1, set2.getSize());
        assertEquals(0, set2.cardinality());

        set1.set();
        assertEquals(1, set1.cardinality());
        assertEquals(0, set2.cardinality());
        set2.clear();
        assertEquals(1, set1.cardinality());
        assertEquals(0, set2.cardinality());
    }

    @Test
    public void smallAndBig() {
        final ArrayBitSetPool pool = allocate();

        final ArrayBitSet set1 = pool.borrowSet(1);
        assertEquals(1, set1.getSize());
        assertEquals(0, set1.cardinality());

        set1.set();
        assertEquals(1, set1.cardinality());
        pool.returnSet(set1);

        final ArrayBitSet set2 = pool.borrowSet(1024);
        assertEquals(1024, set2.getSize());
        assertEquals(0, set2.cardinality());
    }

    @Test
    public void bigAndSmall() {
        final ArrayBitSetPool pool = allocate();

        final ArrayBitSet set1 = pool.borrowSet(1024);
        assertEquals(1024, set1.getSize());
        assertEquals(0, set1.cardinality());

        set1.set();
        assertEquals(1024, set1.cardinality());
        pool.returnSet(set1);

        final ArrayBitSet set2 = pool.borrowSet(1);
        assertEquals(1, set2.getSize());
        assertEquals(0, set2.cardinality());
    }
}
