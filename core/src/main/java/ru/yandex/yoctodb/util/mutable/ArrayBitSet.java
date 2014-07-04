/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable;

import org.jetbrains.annotations.NotNull;

/**
 * Array-based {@link BitSet} implementation
 *
 * @author incubos
 */
public interface ArrayBitSet extends BitSet {
    @NotNull
    long[] toArray();
}
