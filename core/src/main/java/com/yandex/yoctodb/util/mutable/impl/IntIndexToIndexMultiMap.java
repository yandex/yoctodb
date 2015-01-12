/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.Ints;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

/**
 * {@link IntIndexToIndexMultiMap} implementation based on {@link Integer}s
 *
 * @author incubos
 */
@NotThreadSafe
public final class IntIndexToIndexMultiMap implements IndexToIndexMultiMap {
    private final Multimap<Integer, Integer> map = TreeMultimap.create();

    @Override
    public void add(final int key, final int value) {
        if (key < 0)
            throw new IllegalArgumentException("Negative key");
        if (value < 0)
            throw new IllegalArgumentException("Negative value");

        map.put(key, value);
    }

    @Override
    public long getSizeInBytes() {
        if (map.isEmpty())
            throw new IllegalStateException("Empty multimap");

        return 4 + // type
                4 + // keys count
                (4 + 4) * (long) map.keySet().size() + // offsets
                4L * map.size();    // sets
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        if (map.isEmpty())
            throw new IllegalStateException("Empty multimap");

        // Type
        os.write(
                Ints.toByteArray(
                        V1DatabaseFormat.MultiMapType.LIST_BASED.getCode()));

        // Keys count
        os.write(Ints.toByteArray(map.keySet().size()));

        // Offsets
        int offset = 0;
        for (Collection<Integer> value : map.asMap().values()) {
            os.write(Ints.toByteArray(offset));
            offset += 4 + 4 * value.size();
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
                "map=" + map +
                '}';
    }
}
