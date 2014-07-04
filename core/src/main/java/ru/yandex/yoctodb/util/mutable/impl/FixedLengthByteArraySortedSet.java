/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
 * {@link ByteArraySortedSet} with fixed size elements
 *
 * @author incubos
 */
@NotThreadSafe
public final class FixedLengthByteArraySortedSet
        extends AbstractByteArraySortedSet {
    private int elementSize = -1;

    @NotNull
    @Override
    public UnsignedByteArray add(
            @NotNull
            final UnsignedByteArray e) {
        assert e.length() > 0;

        if (elementSize == -1) {
            elementSize = e.length();
        }

        assert e.length() == elementSize :
                "Element length <" + e.length() +
                "> is not equal to expected <" + elementSize + ">";

        return super.add(e);
    }

    @Override
    public int getSizeInBytes() {
        if (!frozen) {
            build();
        }

        assert !sortedElements.isEmpty();

        return 4 + // Element size
               4 + // Element count
               elementSize * sortedElements.size();
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        if (!frozen) {
            build();
        }

        assert !sortedElements.isEmpty();

        // Element size
        os.write(Ints.toByteArray(elementSize));

        // Element count
        os.write(Ints.toByteArray(sortedElements.size()));

        // Elements
        for (UnsignedByteArray e : sortedElements.keySet())
            e.writeTo(os);
    }

    @Override
    public String toString() {
        return "FixedLengthByteArraySortedSet{" +
               "elementsCount=" +
               (frozen ? sortedElements.size() : elements.size()) +
               ", elementSize=" + elementSize +
               '}';
    }
}
