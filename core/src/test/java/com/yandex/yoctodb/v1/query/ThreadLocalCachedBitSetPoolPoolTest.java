/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.query;

import com.yandex.yoctodb.query.BitSetPool;
import com.yandex.yoctodb.query.BitSetPoolPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ThreadLocalCachedBitSetPoolPool}
 *
 * @author incubos
 */
public class ThreadLocalCachedBitSetPoolPoolTest {
    private final int BIT_SET_SIZE = 1;

    @Test
    public void borrow() throws Exception {
        new ThreadLocalCachedBitSetPoolPool(BIT_SET_SIZE, 1).borrowPool();
    }

    @Test(expected = IllegalStateException.class)
    public void borrowOnlyOnce() throws Exception {
        final BitSetPoolPool poolPool =
                new ThreadLocalCachedBitSetPoolPool(BIT_SET_SIZE, 1);
        poolPool.borrowPool();
        poolPool.borrowPool();
    }

    @Test
    public void borrowReturnAndBorrow() throws Exception {
        final BitSetPoolPool poolPool =
                new ThreadLocalCachedBitSetPoolPool(BIT_SET_SIZE, 1);
        final BitSetPool pool = poolPool.borrowPool();
        poolPool.returnPool(pool);
        poolPool.borrowPool();
    }

    @Test
    public void reuseBitSets() {
        final BitSetPoolPool poolPool =
                new ThreadLocalCachedBitSetPoolPool(BIT_SET_SIZE, 1);
        final BitSetPool pool1 = poolPool.borrowPool();
        final BitSet s1 = pool1.borrowSet();
        pool1.returnSet(s1);
        poolPool.returnPool(pool1);
        final BitSetPool pool2 = poolPool.borrowPool();
        final BitSet s2 = pool2.borrowSet();
        assertEquals(s1, s2);
    }
}
