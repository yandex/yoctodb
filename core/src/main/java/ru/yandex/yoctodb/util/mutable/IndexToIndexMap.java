/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import ru.yandex.yoctodb.util.OutputStreamWritable;

/**
 * Map from index to index
 *
 * @author incubos
 */
@NotThreadSafe
public interface IndexToIndexMap extends OutputStreamWritable {
    void put(int key, int value);
}
