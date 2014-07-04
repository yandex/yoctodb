/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable.impl;

import com.google.common.primitives.Ints;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.mutable.ByteArraySortedSet;

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
        maxElement = Math.max(maxElement, e.length());

        return super.add(e);
    }

    @Override
    public int getSizeInBytes() {
        if (!frozen) {
            build();
        }

        assert !sortedElements.isEmpty();

        int elementSize = 0;
        for (UnsignedByteArray e : sortedElements.keySet())
            elementSize += e.length();

        return 4 + // Max element size
               4 + // Element count
               4 * (sortedElements.size() + 1) + // Element offsets
               elementSize; // Element array size
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        if (!frozen) {
            build();
        }

        assert !sortedElements.isEmpty();

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
