/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.immutable.ByteArrayIndexedList;

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
    private final Buffer elements;
    @NotNull
    private final Buffer offsets;

    @NotNull
    public static ByteArrayIndexedList from(
            @NotNull
            final Buffer buf) {
        final int elementsCount = buf.getInt();
        final Buffer offsets = buf.slice((elementsCount + 1) << 2);
        final Buffer elements =
                buf.slice().position(offsets.remaining()).slice();

        return new VariableLengthByteArrayIndexedList(
                elementsCount,
                offsets,
                elements);
    }

    private VariableLengthByteArrayIndexedList(
            final int elementCount,
            @NotNull
            final Buffer offsets,
            @NotNull
            final Buffer elements) {
        if (elementCount <= 0)
            throw new IllegalArgumentException("Non positive element count");
        if (!offsets.hasRemaining())
            throw new IllegalArgumentException("Empty offsets");
        if (!elements.hasRemaining())
            throw new IllegalArgumentException("Empty elements");

        this.elementCount = elementCount;
        this.elements = elements;
        this.offsets = offsets;
    }

    @NotNull
    @Override
    public Buffer get(final int i) {
        assert 0 <= i && i < elementCount;

        final int start = offsets.getInt(i << 2);
        final int end = offsets.getInt((i + 1) << 2);

        assert start < end;

        return elements.slice(start, end - start);
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
