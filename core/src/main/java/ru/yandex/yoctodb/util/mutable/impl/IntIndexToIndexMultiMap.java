/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.util.mutable.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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
        assert key >= 0;
        assert value >= 0;

        map.put(key, value);
    }

    @Override
    public int getSizeInBytes() {
        return  4 + //type
                4 + //keys count
                (4 + 4) * map.keySet().size() + //offsets
                4 * map.size();    //sets
    }

    @Override
    public void writeTo(
            @NotNull
            final
            OutputStream os) throws IOException {
        final byte[] buf = new byte[4];
        //type
        os.write(ByteBuffer.wrap(buf).putInt(V1DatabaseFormat.MultiMapType.LIST_BASED.getCode()).array());

        // Keys count
        os.write(ByteBuffer.wrap(buf).putInt(map.keySet().size()).array());

        // Offsets
        int offset = 0;
        for (Collection<Integer> value : map.asMap().values()) {
            os.write(ByteBuffer.wrap(buf).putInt(offset).array());

            offset += 4 + 4 * value.size();
        }

        // Sets
        for (Collection<Integer> value : map.asMap().values()) {
            os.write(ByteBuffer.wrap(buf).putInt(value.size()).array());

            for (Integer v : value) {
                os.write(ByteBuffer.wrap(buf).putInt(v).array());
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
