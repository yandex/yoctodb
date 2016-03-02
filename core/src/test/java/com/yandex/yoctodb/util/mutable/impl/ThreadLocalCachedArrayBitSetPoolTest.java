/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.AbstractArrayBitSetPoolTestBench;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;

/**
 * Unit tests for {@link ThreadLocalCachedArrayBitSetPool}
 *
 * @author incubos
 */
public class ThreadLocalCachedArrayBitSetPoolTest
        extends AbstractArrayBitSetPoolTestBench {
    @Override
    protected ArrayBitSetPool allocate() {
        return new ThreadLocalCachedArrayBitSetPool();
    }
}
