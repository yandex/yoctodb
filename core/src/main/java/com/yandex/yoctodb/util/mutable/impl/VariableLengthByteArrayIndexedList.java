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
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link ByteArrayIndexedList} with variable sized elements
 *
 * @author incubos
 */
@NotThreadSafe
public final class VariableLengthByteArrayIndexedList
        implements ByteArrayIndexedList {
    @NotNull
    private final Collection<UnsignedByteArray> elements;
    private final long elementSize;

    public VariableLengthByteArrayIndexedList(
            @NotNull
            final Collection<UnsignedByteArray> elements) {
        this.elements = elements;
        long elementSize = 0;
        for (UnsignedByteArray element : elements) {
            elementSize += element.length();
        }
        this.elementSize = elementSize;
    }

    @Override
    public long getSizeInBytes() {
        return 4L + // Element count
               8L * (elements.size() + 1L) + // Element offsets
               elementSize; // Element array size
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        // Element count
        os.write(Ints.toByteArray(elements.size()));

        final Map<OutputStreamWritable, Long> valueOffset = new HashMap<>();
        // Element offsets
        long elementOffset = 0;
        for (OutputStreamWritable e : elements) {
            if (valueOffset.containsKey(e)) {
                os.write(Longs.toByteArray(valueOffset.get(e)));
            } else {
                valueOffset.put(e, elementOffset);
                os.write(Longs.toByteArray(elementOffset));
                elementOffset += e.getSizeInBytes();
            }
        }
        os.write(Longs.toByteArray(elementOffset));

        // Elements
        for (OutputStreamWritable e : elements)
            e.writeTo(os);
    }

    @Override
    public String toString() {
        return "VariableLengthByteArrayIndexedList{" +
               "elementsCount=" + elements.size() +
               '}';
    }
}
