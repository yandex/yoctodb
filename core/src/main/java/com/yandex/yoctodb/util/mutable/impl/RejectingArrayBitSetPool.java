package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.ArrayBitSet;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import org.jetbrains.annotations.NotNull;

/**
 * Rejecting {@link ArrayBitSetPool} implementation
 *
 * @author incubos
 */
public final class RejectingArrayBitSetPool implements ArrayBitSetPool {
    public static final ArrayBitSetPool INSTANCE =
            new RejectingArrayBitSetPool();

    private RejectingArrayBitSetPool() {
        // Do nothing
    }

    @NotNull
    @Override
    public ArrayBitSet borrowSet(final int size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void returnSet(
            @NotNull
            final ArrayBitSet set) {
        throw new UnsupportedOperationException();
    }
}
