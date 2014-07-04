/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable.impl;

import ru.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;

/**
 * @author svyatoslav
 */
public class IndexToIndexMultiMapFactory {

    public static final int UNIQUE_VALUES_THRESHOLD = 30;

    public static IndexToIndexMultiMap buildIndexToIndexMultiMap(final int documentsCount,
                                                                 final int uniqueValuesCount) {
        if (uniqueValuesCount < UNIQUE_VALUES_THRESHOLD) {
            return new BitSetMultiMap(documentsCount);
        } else {
            return new IntIndexToIndexMultiMap();
        }
    }
}
