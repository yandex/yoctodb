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

import com.google.common.primitives.Ints;
import com.yandex.yoctodb.util.mutable.IndexToIndexMap;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
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
        if (key < 0)
            throw new IllegalArgumentException("Negative key");
        if (value < 0)
            throw new IllegalArgumentException("Negative value");

        final Integer previous = elements.put(key, value);

        if (previous != null)
            throw new IllegalArgumentException(
                    "Key <" + key + "> was already bound to <" + previous +
                    ">");
    }

    @Override
    public long getSizeInBytes() {
        return 4L + 4L * elements.size();
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        // Elements count
        os.write(Ints.toByteArray(elements.size()));

        // Values
        int index = 0;
        for (Map.Entry<Integer, Integer> entry : elements.entrySet()) {
            if (entry.getKey() != index) {
                throw new IllegalStateException("Indexes are not continuous");
            }

            os.write(Ints.toByteArray(entry.getValue()));

            index++;
        }
    }

    @Override
    public String toString() {
        return "IntIndexToIndexMap{" +
               "elements=" + elements.size() +
               '}';
    }
}
