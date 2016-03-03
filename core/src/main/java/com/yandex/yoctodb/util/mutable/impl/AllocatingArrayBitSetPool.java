package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import org.jetbrains.annotations.NotNull;

/**
 * Noncaching {@link ArrayBitSetPool} implementation
 *
 * @author incubos
 */
public final class AllocatingArrayBitSetPool implements ArrayBitSetPool {
    public static final ArrayBitSetPool INSTANCE =
            new AllocatingArrayBitSetPool();

    private AllocatingArrayBitSetPool() {
        // Do nothing
    }

    @NotNull
    @Override
    public ArrayBitSet borrowSet(final int size) {
        return LongArrayBitSet.zero(size);
    }

    @Override
    public void returnSet(
            @NotNull
            final ArrayBitSet set) {
        // Do nothing
    }
}
