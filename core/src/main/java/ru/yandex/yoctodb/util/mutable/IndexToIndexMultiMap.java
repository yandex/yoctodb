/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import ru.yandex.yoctodb.util.OutputStreamWritable;

/**
 * Integer to Integer multi map
 *
 * @author incubos
 */
@NotThreadSafe
public interface IndexToIndexMultiMap extends OutputStreamWritable {
    void add(int key, int value);
}
