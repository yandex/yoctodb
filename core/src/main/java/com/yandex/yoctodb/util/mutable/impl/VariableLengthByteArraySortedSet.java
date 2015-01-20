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
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link ByteArraySortedSet} with variable sized elements
 *
 * @author incubos
 */
@NotThreadSafe
public final class VariableLengthByteArraySortedSet
        extends AbstractByteArraySortedSet {
    private int maxElement = 0;

    @NotNull
    @Override
    public UnsignedByteArray add(
            @NotNull
            final UnsignedByteArray e) {
        if (e.isEmpty())
            throw new IllegalArgumentException("Empty element");

        maxElement = Math.max(maxElement, e.length());

        return super.add(e);
    }

    @Override
    public long getSizeInBytes() {
        if (!frozen) {
            build();
        }

        if (sortedElements.isEmpty())
            throw new IllegalStateException("Empty set");

        int elementSize = 0;
        for (UnsignedByteArray e : sortedElements.keySet())
            elementSize += e.length();

        return 4 + // Max element size
                4 + // Element count
                4L * (sortedElements.size() + 1) + // Element offsets
                (long) elementSize; // Element array size
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        if (!frozen) {
            build();
        }

        if (sortedElements.isEmpty())
            throw new IllegalStateException("Empty set");

        // Max element
        os.write(Ints.toByteArray(maxElement));

        // Element count
        os.write(Ints.toByteArray(sortedElements.size()));

        // Element offsets
        int elementOffset = 0;
        for (UnsignedByteArray e : sortedElements.keySet()) {
            os.write(Ints.toByteArray(elementOffset));
            elementOffset += e.length();
        }
        os.write(Ints.toByteArray(elementOffset));

        // Elements
        for (UnsignedByteArray e : sortedElements.keySet())
            e.writeTo(os);
    }

    @Override
    public String toString() {
        return "VariableLengthByteArraySortedSet{" +
               "elementsCount=" +
               (frozen ? sortedElements.size() : elements.size()) +
               ", maxElement=" + maxElement +
               '}';
    }
}
