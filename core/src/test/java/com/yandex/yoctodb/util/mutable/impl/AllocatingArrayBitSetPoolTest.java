package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.AbstractArrayBitSetPoolTestBench;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;

/**
 * Unit tests for {@link AllocatingArrayBitSetPool}
 *
 * @author incubos
 */
public class AllocatingArrayBitSetPoolTest
        extends AbstractArrayBitSetPoolTestBench {
    @Override
    protected ArrayBitSetPool allocate() {
        return AllocatingArrayBitSetPool.INSTANCE;
    }
}
