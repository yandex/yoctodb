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
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

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
        if (elementSize == -1) {
            elementSize = e.length();
        }

        if (e.length() != elementSize)
            throw new IllegalArgumentException(
                    "Element length <" + e.length() +
                    "> is not equal to expected <" + elementSize + ">");

        return super.add(e);
    }

    @Override
    public long getSizeInBytes() {
        if (sortedElements == null) {
            build();
        }

        return 4L + // Element size
               4L + // Element count
               ((long) elementSize) * sortedElements.size();
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        if (sortedElements == null) {
            build();
        }

        // Element size
        os.write(Ints.toByteArray(elementSize));

        // Element count
        os.write(Ints.toByteArray(sortedElements.size()));

        // Elements
        for (OutputStreamWritable e : sortedElements.keySet())
            e.writeTo(os);
    }

    @Override
    public String toString() {
        return "FixedLengthByteArraySortedSet{" +
               "elementsCount=" +
               (sortedElements == null ? elements.size() : sortedElements.size()) +
               ", elementSize=" + elementSize +
               '}';
    }
}
