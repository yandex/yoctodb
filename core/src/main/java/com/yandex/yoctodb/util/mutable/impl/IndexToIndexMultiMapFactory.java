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

import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;

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
