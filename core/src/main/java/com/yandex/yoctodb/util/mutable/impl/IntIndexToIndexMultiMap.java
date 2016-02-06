/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

/**
 * {@link IntIndexToIndexMultiMap} implementation based on {@link Integer}s
 *
 * Format:
 *
 * <pre>
 * {@code
 * type (int)
 * keys count (int)
 * offsets
 *   offset1 (long)
 *   offset2 (long)
 *   ...
 * sets
 *   set1
 *     size (int)
 *     value1 (int)
 *     value2 (int)
 *     ...
 *   set2
 *     size (int)
 *     value1 (int)
 *     value2 (int)
 *     ...
 * }
 * </pre>
 *
 * @author incubos
 */
@NotThreadSafe
public final class IntIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final Collection<? extends Collection<Integer>> map;
    private final long sizeInBytes;

    public IntIndexToIndexMultiMap(
            @NotNull
            final Collection<? extends Collection<Integer>> map) {
        this.map = map;
        long elements = 0;
        for (Collection<Integer> ids : map) {
            elements += ids.size();
        }
        this.sizeInBytes =
                4L + // type
                4L + // keys count
                (8L + 4L) * map.size() + // offsets + sizes
                4L * elements;    // set elements
    }

    @Override
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        // Type
        os.write(
                Ints.toByteArray(
                        V1DatabaseFormat.MultiMapType.LIST_BASED.getCode()));

        // Keys count
        os.write(Ints.toByteArray(map.size()));

        // Offsets
        long offset = 0L;
        for (Collection<Integer> value : map) {
            os.write(Longs.toByteArray(offset));
            offset += 4L + 4L * value.size();
        }

        // Sets
        for (Collection<Integer> value : map) {
            os.write(Ints.toByteArray(value.size()));

            for (Integer v : value) {
                assert v >= 0;
                os.write(Ints.toByteArray(v));
            }
        }
    }

    @Override
    public String toString() {
        return "IntIndexToIndexMultiMap{" +
               "keys=" + map.size() +
               '}';
    }
}
