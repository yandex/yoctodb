/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.immutable.impl;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.immutable.ByteArrayIndexedList;

import java.nio.ByteBuffer;

/**
 * Fixed length immutable implementation of {@link ByteArrayIndexedList}
 *
 * @author incubos
 */
@Immutable
public class FixedLengthByteArrayIndexedList
        implements ByteArrayIndexedList {
    private final int elementSize;
    private final int elementCount;
    private final ByteBuffer elements;

    @NotNull
    public static ByteArrayIndexedList from(
            @NotNull
            final ByteBuffer buf) {
        final int elementSize = buf.getInt();
        final int elementCount = buf.getInt();

        return new FixedLengthByteArrayIndexedList(
                elementSize,
                elementCount,
                buf.slice());
    }

    private FixedLengthByteArrayIndexedList(
            final int elementSize,
            final int elementCount,
            final ByteBuffer elements) {
        assert elementSize > 0;
        assert elementCount > 0;
        assert elements.hasRemaining();

        this.elementSize = elementSize;
        this.elementCount = elementCount;
        this.elements = elements;
    }

    @NotNull
    @Override
    public ByteBuffer get(final int i) {
        assert 0 <= i && i < elementCount;

        final ByteBuffer buf = elements.duplicate();
        buf.position(i * elementSize);
        buf.limit(buf.position() + elementSize);

        return buf.slice();
    }

    @Override
    public int size() {
        return elementCount;
    }

    @Override
    public String toString() {
        return "FixedLengthByteArrayIndexedList{" +
                "elementSize=" + elementSize +
                ", elementCount=" + elementCount +
                '}';
    }
}
