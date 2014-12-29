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

import com.google.common.primitives.Ints;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link ByteArrayIndexedList} with variable sized elements
 *
 * @author incubos
 */
@NotThreadSafe
public final class VariableLengthByteArrayIndexedList
        implements ByteArrayIndexedList {
    private final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();

    @Override
    public void add(
            @NotNull
            final UnsignedByteArray e) {
        if (e.isEmpty())
            throw new IllegalArgumentException("Empty element");

        elements.add(e);
    }

    @Override
    public int getSizeInBytes() {
        if (elements.isEmpty())
            throw new IllegalStateException("Empty list");

        int elementSize = 0;
        for (UnsignedByteArray e : elements)
            elementSize += e.length();

        return 4 + // Element count
                4 * (elements.size() + 1) + // Element offsets
                elementSize; // Element array size
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        if (elements.isEmpty())
            throw new IllegalStateException("Empty list");

        // Element count
        os.write(Ints.toByteArray(elements.size()));

        // Element offsets
        int elementOffset = 0;
        for (UnsignedByteArray e : elements) {
            os.write(Ints.toByteArray(elementOffset));
            elementOffset += e.length();
        }
        os.write(Ints.toByteArray(elementOffset));

        // Elements
        for (UnsignedByteArray e : elements)
            e.writeTo(os);
    }

    @Override
    public String toString() {
        return "VariableLengthByteArrayIndexedList{" +
                "elementsCount=" + elements.size() +
                '}';
    }
}
