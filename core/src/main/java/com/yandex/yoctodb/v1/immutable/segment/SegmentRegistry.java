/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides means to register segment readers and build segments
 *
 * @author incubos
 */
@ThreadSafe
public final class SegmentRegistry {
    private SegmentRegistry() {
        // Can't construct
    }

    // For test coverage
    static {
        new SegmentRegistry();
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
            final Buffer buffer) throws IOException {
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
    }
}
