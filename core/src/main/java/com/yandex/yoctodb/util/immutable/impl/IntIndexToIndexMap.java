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
import com.yandex.yoctodb.util.immutable.IndexToIndexMap;

/**
 * @author svyatoslav
 */
@Immutable
public class IntIndexToIndexMap implements IndexToIndexMap {
    @NotNull
    private final Buffer elements;
    private final int elementCount;

    private IntIndexToIndexMap(
            final int elementCount,
            @NotNull
            final Buffer elements) {
        if (elementCount <= 0)
            throw new IllegalArgumentException("Non positive element count");
        if (!elements.hasRemaining())
            throw new IllegalArgumentException("Empty elements");

        this.elementCount = elementCount;
        this.elements = elements;
    }

    @NotNull
    public static IndexToIndexMap from(
            @NotNull
            final Buffer buf) {
        final int elementsCount = buf.getInt();
        final Buffer elements = buf.slice();

        return new IntIndexToIndexMap(
                elementsCount,
                elements.slice());
    }

    @Override
    public int get(final int key) {
        assert 0 <= key && key < elementCount;

        return elements.getInt(key << 2);
    }

    @Override
    public int size() {
        return elementCount;
    }

    @Override
    public String toString() {
        return "IntIndexToIndexMap{" +
                "elementCount=" + elementCount +
                '}';
    }
}
