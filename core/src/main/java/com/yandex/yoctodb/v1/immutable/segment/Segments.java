/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable.segment;

import com.yandex.yoctodb.util.buf.Buffer;
import org.jetbrains.annotations.NotNull;

/**
 * Utility methods for segments
 *
 * @author incubos
 */
final class Segments {
    private Segments() {
        //
    }

    // For test coverage
    static {
        new Segments();
    }

    /**
     * Reads {@code long} segment size, extracts the slice and advances
     * {@code from} buffer
     *
     * @param from buffer to extract segment slice from
     * @return segment slice
     */
    @NotNull
    static Buffer extract(
            @NotNull
            final Buffer from) {
        final long size = from.getLong();
        final Buffer result = from.slice(size);
        from.advance(size);
        return result;
    }

    /**
     * Reads {@code int} string size, extracts the string and advances
     * {@code from} buffer
     *
     * @param from buffer to extract the string from
     * @return extracted string
     */
    @NotNull
    static String extractString(
            @NotNull
            final Buffer from) {
        final int size = from.getInt();
        final byte[] buffer = new byte[size];
        from.get(buffer);

        return new String(buffer);
    }
}
