package com.yandex.yoctodb.util.mutable.impl;

import org.junit.Test;

/**
 * Unit tests for {@link RejectingArrayBitSetPool}
 *
 * @author incubos
 */
public class RejectingArrayBitSetPoolTest {
    @Test(expected = UnsupportedOperationException.class)
    public void rejectBorrow() {
        RejectingArrayBitSetPool.INSTANCE.borrowSet(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void rejectReturn() {
        RejectingArrayBitSetPool.INSTANCE.returnSet(LongArrayBitSet.one(1));
    }
}
