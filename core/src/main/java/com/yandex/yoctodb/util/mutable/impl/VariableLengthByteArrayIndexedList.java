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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
    private final List<UnsignedByteArray> uniqueElements;
    private final Map<UnsignedByteArray, Long> valueOffset;
    private final long lastOffsetValue;
    private final long uniqueElementsSize;


    public VariableLengthByteArrayIndexedList(
            @NotNull final Collection<UnsignedByteArray> elements) {
        this.elements = elements;
        this.valueOffset = new HashMap<>();
        this.uniqueElements = new ArrayList<>();

        long elementOffset = 0;
        for (UnsignedByteArray e : elements) {
            if (!valueOffset.containsKey(e)) {
                valueOffset.put(e, elementOffset);
                elementOffset += e.getSizeInBytes();
                uniqueElements.add(e);
            }
        }
        this.lastOffsetValue = elementOffset;
        long elementSize = 0;
        for (UnsignedByteArray element : uniqueElements) {
            elementSize += element.length();
        }
        this.uniqueElementsSize = elementSize;
    }

    @Override
    public long getSizeInBytes() {
        return 4L + // Element count
                8L * (elements.size() + 1L) + // Element offsets
                uniqueElementsSize; // Element array size
    }

    @Override
    public void writeTo(
            @NotNull final OutputStream os) throws IOException {
        // Element count
        os.write(Ints.toByteArray(uniqueElements.size()));

        // Element offsets
        for (UnsignedByteArray e : elements) {
            os.write(Longs.toByteArray(valueOffset.get(e)));
        }
        os.write(Longs.toByteArray(lastOffsetValue));

        // Elements
        for (OutputStreamWritable e : uniqueElements)
            e.writeTo(os);
    }

    @Override
    public String toString() {
        return "VariableLengthByteArrayIndexedList{" +
               "elementsCount=" + elements.size() +
               '}';
    }
}
