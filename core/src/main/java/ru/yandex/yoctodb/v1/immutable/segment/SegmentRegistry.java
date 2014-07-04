/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.v1.immutable.segment;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides means to register segment readers and build segments
 *
 * @author incubos
 */
@ThreadSafe
public class SegmentRegistry {
    private SegmentRegistry() {
        // Can't construct
    }

    // Segment readers
    private final static ConcurrentMap<Integer, SegmentReader> readers =
            new ConcurrentHashMap<Integer, SegmentReader>();

    public static void register(
            final int type,
            @NotNull
            final SegmentReader reader) {
        final SegmentReader previous = readers.putIfAbsent(type, reader);
        if (previous != null) {
            throw new IllegalArgumentException(
                    "Duplicate segment readers with type " + type);
        }
    }

    @NotNull
    public static Segment read(
            final int type,
            @NotNull
            final ByteBuffer buffer) throws IOException {
        final SegmentReader reader = readers.get(type);
        if (reader == null) {
            throw new NoSuchElementException(
                    "No segment reader with type " + type);
        }

        return reader.read(buffer);
    }

    static {
        // Register readers for default segments
        V1FilterableIndex.registerReader();
        V1FullIndex.registerReader();
        V1PayloadSegment.registerReader();
        V1TrieBasedFilterableIndex.registerReader();
    }
}
