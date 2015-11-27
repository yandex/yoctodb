/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.v1.V1DatabaseFormat;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

/**
 * {@link IntIndexToIndexMultiMap} implementation based on {@link Integer}s
 * <p/>
 * Format:
 * <p/>
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
    private final Multimap<Integer, Integer> map = TreeMultimap.create();

    @Override
    public void put(final int key, final int value) {
        if (key < 0)
            throw new IllegalArgumentException("Negative key");
        if (value < 0)
            throw new IllegalArgumentException("Negative value");

        map.put(key, value);
    }

    @Override
    public long getSizeInBytes() {
        return 4L + // type
               4L + // keys count
               8L * map.keySet().size() + // offsets
               4L * map.keySet().size() + // sizes
               4L * map.size();    // set elements
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
        os.write(Ints.toByteArray(map.keySet().size()));

        // Offsets
        long offset = 0L;
        int index = 0;
        for (Map.Entry<Integer, Collection<Integer>> entry :
                map.asMap().entrySet()) {
            if (entry.getKey() != index) {
                throw new IllegalStateException("Indexes are not continuous");
            }

            os.write(Longs.toByteArray(offset));
            offset += 4L + 4L * entry.getValue().size();

            index++;
        }

        // Sets
        for (Collection<Integer> value : map.asMap().values()) {
            os.write(Ints.toByteArray(value.size()));

            for (Integer v : value) {
                os.write(Ints.toByteArray(v));
            }
        }
    }

    @Override
    public String toString() {
        return "IntIndexToIndexMultiMap{" +
               "keys=" + map.keySet().size() +
               ", values" + map.size() +
               '}';
    }
}
