/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
