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
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.SortedSet;

/**
 * {@link ByteArraySortedSet} with fixed size elements
 *
 * @author incubos
 */
@NotThreadSafe
public final class FixedLengthByteArraySortedSet
        extends AbstractByteArraySortedSet {
    public FixedLengthByteArraySortedSet(
            final SortedSet<UnsignedByteArray> elements) {
        super(elements);

        if (elements.isEmpty())
            throw new IllegalArgumentException("Empty set");
    }

    @Override
    public long getSizeInBytes() {
        return 4L + // Element size
               4L + // Element count
               elements.first().getSizeInBytes() * elements.size();
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        assert elements.first().getSizeInBytes() <= Integer.MAX_VALUE;

        // Element size
        os.write(Ints.toByteArray((int) elements.first().getSizeInBytes()));

        // Element count
        os.write(Ints.toByteArray(elements.size()));

        // Elements
        long elementSize = -1;
        for (UnsignedByteArray e : elements) {
            if (elementSize == -1)
                elementSize = e.getSizeInBytes();

            assert elementSize == e.getSizeInBytes() : "Variable size";

            e.writeTo(os);
        }
    }

    @Override
    public String toString() {
        return "FixedLengthByteArraySortedSet{" +
               "elementsCount=" + elements.size() +
               ", elementSize=" + elements.first().getSizeInBytes() +
               '}';
    }
}
