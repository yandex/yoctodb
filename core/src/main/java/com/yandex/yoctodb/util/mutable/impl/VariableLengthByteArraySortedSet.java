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
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.SortedSet;

/**
 * {@link ByteArraySortedSet} with variable sized elements
 *
 * @author incubos
 */
@NotThreadSafe
public final class VariableLengthByteArraySortedSet
        extends AbstractByteArraySortedSet {
    private final long sizeInBytes;

    public VariableLengthByteArraySortedSet(
            @NotNull
            final SortedSet<UnsignedByteArray> elements) {
        super(elements);

        long elementSize = 0;
        for (OutputStreamWritable e : elements)
            elementSize += e.getSizeInBytes();

        this.sizeInBytes =
                4L + // Element count
                8L * (elements.size() + 1) + // Element offsets
                elementSize; // Element array size
    }

    @Override
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        // Element count
        os.write(Ints.toByteArray(elements.size()));

        // Element offsets
        long elementOffset = 0;
        for (OutputStreamWritable e : elements) {
            os.write(Longs.toByteArray(elementOffset));
            elementOffset += e.getSizeInBytes();
        }
        os.write(Longs.toByteArray(elementOffset));

        // Elements
        for (OutputStreamWritable e : elements)
            e.writeTo(os);
    }

    @Override
    public String toString() {
        return "VariableLengthByteArraySortedSet{" +
               "elementsCount=" + elements.size() +
               '}';
    }
}
