/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.immutable.impl;

import ru.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

import java.nio.ByteBuffer;

/**
 * @author svyatoslav
 */
public class IndexToIndexMultiMapReader {

    public static IndexToIndexMultiMap from(ByteBuffer byteBuffer) {
        final int type = byteBuffer.getInt();
        if (type == V1DatabaseFormat.MultiMapType.LIST_BASED.getCode()) {
            return IntIndexToIndexMultiMap.from(byteBuffer.slice());
        }
        if (type == V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode()) {
            return BitSetBasedIndexToIndexMultiMap.from(byteBuffer.slice());
        }
        return null;
    }
}
