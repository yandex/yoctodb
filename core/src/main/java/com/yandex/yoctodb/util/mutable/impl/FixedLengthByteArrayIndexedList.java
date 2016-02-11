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
import com.yandex.yoctodb.util.OutputStreamWritable;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.mutable.ByteArrayIndexedList;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

/**
 * {@link ByteArrayIndexedList} with fixed size elements
 *
 * @author incubos
 */
@NotThreadSafe
public final class FixedLengthByteArrayIndexedList
        implements ByteArrayIndexedList {
    @NotNull
    private final Collection<UnsignedByteArray> elements;

    private int elementSize = -1;

    public FixedLengthByteArrayIndexedList(
            @NotNull
            final Collection<UnsignedByteArray> elements) {
        assert !elements.isEmpty();

        this.elements = elements;
        this.elementSize = elements.iterator().next().length();
    }

    @Override
    public long getSizeInBytes() {
        return 4L + // Element size
               4L + // Element count
               ((long) elementSize) * elements.size();
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        // Element size
        os.write(Ints.toByteArray(elementSize));

        // Element count
        os.write(Ints.toByteArray(elements.size()));

        // Elements
        for (OutputStreamWritable e : elements) {
            assert e.getSizeInBytes() == elementSize : "Wrong size";
            e.writeTo(os);
        }
    }

    @Override
    public String toString() {
        return "FixedLengthByteArrayIndexedList{" +
               "elementsCount=" + elements.size() +
               ", elementSize=" + elementSize +
               '}';
    }
}
