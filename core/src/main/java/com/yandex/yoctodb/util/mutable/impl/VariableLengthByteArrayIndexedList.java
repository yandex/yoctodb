/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
        assert e.length() > 0;

        elements.add(e);
    }

    @Override
    public int getSizeInBytes() {
        assert !elements.isEmpty();

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
        assert !elements.isEmpty();
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
