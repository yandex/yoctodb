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

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.mutable.IndexToIndexMap;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@link IndexToIndexMap} implementation based on {@link Integer}s
 *
 * @author incubos
 */
@NotThreadSafe
public final class IntIndexToIndexMap implements IndexToIndexMap {
    private final Map<Integer, Integer> elements =
            new TreeMap<Integer, Integer>();

    @Override
    public void put(final int key, final int value) {
        assert key >= 0;
        assert value >= 0;

        final Integer previous = elements.put(key, value);

        assert previous == null;
    }

    @Override
    public int getSizeInBytes() {
        return 4 + 4 * elements.size();
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        final byte[] buf = new byte[4];
        // Elements count
        os.write(ByteBuffer.wrap(buf).putInt(elements.size()).array());

        // Values
        int index = 0;
        for (Map.Entry<Integer, Integer> entry : elements.entrySet()) {
            if (entry.getKey() != index) {
                throw new IllegalStateException("indexes are not continuous");
            }

            os.write(
                    ByteBuffer.wrap(buf)
                              .putInt(entry.getValue())
                              .array());

            index++;
        }
    }

    @Override
    public String toString() {
        return "IntIndexToIndexMap{" +
                "elements=" + elements +
                '}';
    }
}
