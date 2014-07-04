/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.immutable.segment;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reads segments
 *
 * @author incubos
 */
@Immutable
public interface SegmentReader {
    @NotNull
    Segment read(
            @NotNull
            ByteBuffer buffer) throws IOException;
}
