/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.util.immutable;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Indexed list of {@link com.yandex.yoctodb.util.UnsignedByteArray}s
 *
 * @author incubos
 */
@ThreadSafe
public interface ByteArrayIndexedList {
    @NotNull
    ByteBuffer get(int i);

    int size();
}
