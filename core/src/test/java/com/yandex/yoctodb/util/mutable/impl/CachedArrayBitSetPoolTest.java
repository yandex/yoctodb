package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.AbstractArrayBitSetPoolTestBench;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;

/**
 * Unit tests for {@link CachedArrayBitSetPool}
 *
 * @author incubos
 */
public class CachedArrayBitSetPoolTest
        extends AbstractArrayBitSetPoolTestBench {
    @Override
    protected ArrayBitSetPool allocate() {
        return new CachedArrayBitSetPool();
    }
}
