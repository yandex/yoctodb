/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
