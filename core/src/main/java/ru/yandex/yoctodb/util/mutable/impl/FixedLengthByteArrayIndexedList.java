/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable.impl;

import com.google.common.primitives.Ints;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.mutable.ByteArrayIndexedList;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link ByteArrayIndexedList} with fixed size elements
 *
 * @author incubos
 */
@NotThreadSafe
public final class FixedLengthByteArrayIndexedList
        implements ByteArrayIndexedList {
    private final List<UnsignedByteArray> elements = new ArrayList<UnsignedByteArray>();

    private int elementSize = -1;

    @Override
    public void add(
            @NotNull
            final UnsignedByteArray e) {
        assert e.length() > 0;

        elements.add(e);

        if (elementSize == -1) {
            elementSize = e.length();
        }

        assert e.length() == elementSize :
                "Element length <" + e.length() +
                "> is not equal to expected <" + elementSize + ">";
    }

    @Override
    public int getSizeInBytes() {
        assert !elements.isEmpty();

        return 4 + // Element size
               4 + // Element count
               elementSize * elements.size();
    }

    @Override
    public void writeTo(
            @NotNull
            final OutputStream os) throws IOException {
        assert !elements.isEmpty();
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
