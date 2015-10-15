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
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * {@link ByteArrayIndexedList} with fixed size elements
 *
 * @author incubos
 */
@NotThreadSafe
public final class FixedLengthByteArrayIndexedList
        implements ByteArrayIndexedList {
    private final List<UnsignedByteArray> elements =
            new LinkedList<UnsignedByteArray>();

    private int elementSize = -1;

    @Override
    public void add(
            @NotNull
            final UnsignedByteArray e) {
        if (e.isEmpty())
            throw new IllegalArgumentException("Empty element");

        elements.add(e);

        if (elementSize == -1) {
            elementSize = e.length();
        }

        if (e.length() != elementSize)
            throw new IllegalArgumentException(
                    "Element length <" + e.length() +
                            "> is not equal to expected <" + elementSize + ">");
    }

    @Override
    public long getSizeInBytes() {
        if (elements.isEmpty())
            throw new IllegalStateException("Empty list");

        return 4 + // Element size
               4 + // Element count
                (long) elementSize * elements.size();
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        if (elements.isEmpty())
            throw new IllegalStateException("Empty list");

        // Element size
        os.write(Ints.toByteArray(elementSize));

        // Element count
        os.write(Ints.toByteArray(elements.size()));

        // Elements
        for (UnsignedByteArray e : elements)
            e.writeTo(os);
    }

    @Override
    public String toString() {
        return "FixedLengthByteArrayIndexedList{" +
                "elementsCount=" + elements.size() +
                ", elementSize=" + elementSize +
                '}';
    }
}
