/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.util.immutable.impl;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;

import java.nio.ByteBuffer;

/**
 * Variable length immutable implementation of
 * {@link com.yandex.yoctodb.util.immutable.ByteArrayIndexedList}
 *
 * @author svyatoslav
 */
@Immutable
public class VariableLengthByteArrayIndexedList
        implements ByteArrayIndexedList {
    private final int elementCount;
    @NotNull
    private final ByteBuffer elements;
    @NotNull
    private final ByteBuffer offsets;

    @NotNull
    public static ByteArrayIndexedList from(
            @NotNull
            final ByteBuffer buf) {
        final int elementsCount = buf.getInt();
        assert elementsCount > 0;

        final ByteBuffer offsets = buf.slice();
        offsets.limit(4 * (elementsCount + 1));

        final ByteBuffer elements = buf.slice();
        elements.position(offsets.remaining());

        return new VariableLengthByteArrayIndexedList(
                elementsCount,
                offsets.slice(),
                elements.slice());
    }

    private VariableLengthByteArrayIndexedList(
            final int elementCount,
            @NotNull
            final ByteBuffer offsets,
            @NotNull
            final ByteBuffer elements) {
        assert elementCount > 0;
        assert offsets.hasRemaining();
        assert elements.hasRemaining();

        this.elementCount = elementCount;
        this.elements = elements;
        this.offsets = offsets;
    }

    @NotNull
    @Override
    public ByteBuffer get(final int i) {
        assert 0 <= i && i < elementCount;

        final int start = offsets.getInt(i << 2);
        final int end = offsets.getInt((i + 1) << 2);

        assert start < end;

        final ByteBuffer copy = elements.duplicate();
        copy.position(start);
        copy.limit(end);

        return copy.slice();
    }

    @Override
    public int size() {
        return elementCount;
    }

    @Override
    public String toString() {
        return "VariableLengthByteArrayIndexedList{" +
                "elementCount=" + elementCount +
                '}';
    }
}
