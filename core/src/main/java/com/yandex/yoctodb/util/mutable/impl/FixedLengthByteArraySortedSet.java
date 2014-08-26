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
import com.yandex.yoctodb.util.mutable.ByteArraySortedSet;

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
