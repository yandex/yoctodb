/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.util.immutable.impl;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.immutable.IndexToIndexMap;

import java.nio.ByteBuffer;

/**
 * @author svyatoslav
 */
@Immutable
public class IntIndexToIndexMap implements IndexToIndexMap {
    @NotNull
    private final ByteBuffer elements;
    private final int elementsCount;

    private IntIndexToIndexMap(
            final int elementsCount,
            @NotNull
            final ByteBuffer elements) {
        assert elementsCount > 0;
        assert elements.hasRemaining();

        this.elementsCount = elementsCount;
        this.elements = elements;
    }

    @NotNull
    public static IndexToIndexMap from(
            @NotNull
            final ByteBuffer buf) {
        final int elementsCount = buf.getInt();
        final ByteBuffer elements = buf.slice();

        return new IntIndexToIndexMap(
                elementsCount,
                elements.slice());
    }

    @Override
    public int get(final int key) {
        assert 0 <= key && key < elementsCount;

        return elements.getInt(key << 2);
    }

    @Override
    public int size() {
        return elementsCount;
    }

    @Override
    public String toString() {
        return "IntIndexToIndexMap{" +
                "elementsCount=" + elementsCount +
                '}';
    }
}
