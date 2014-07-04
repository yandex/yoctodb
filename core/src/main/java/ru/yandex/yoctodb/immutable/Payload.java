/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.immutable;

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
