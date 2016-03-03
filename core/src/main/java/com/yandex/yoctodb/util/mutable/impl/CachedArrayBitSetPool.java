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

import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * LIFO cache of {@link ArrayBitSet}s
 *
 * @author incubos
 */
@ThreadSafe
public final class CachedArrayBitSetPool extends AbstractCachedArrayBitSetPool {
    @NotNull
    private final Deque<long[]> cache = new ConcurrentLinkedDeque<long[]>();

    public CachedArrayBitSetPool(
            final int sizeHint,
            final float loadFactor) {
        super(sizeHint, loadFactor);
    }

    public CachedArrayBitSetPool() {
        this(DEFAULT_SIZE_HINT, DEFAULT_LOAD_FACTOR);
    }

    @Override
    protected Deque<long[]> getCache() {
        return cache;
    }
}
