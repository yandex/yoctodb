/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import ru.yandex.yoctodb.util.OutputStreamWritable;

/**
 * @author svyatoslav
 */
@NotThreadSafe
public interface BitSetBasedIndexToIndexMultiMap extends OutputStreamWritable {
    void add(int key, ArrayBitSet bitSet);
}
