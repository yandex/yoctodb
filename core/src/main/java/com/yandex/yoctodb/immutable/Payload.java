/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Payload segment interface
 *
 * @author incubos
 */
@Immutable
public interface Payload {
    int getSize();

    @NotNull
    ByteBuffer getPayload(final int i);
}
