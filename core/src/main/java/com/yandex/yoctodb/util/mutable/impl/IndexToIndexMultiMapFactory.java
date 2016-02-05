/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;

/**
 * @author svyatoslav
 */
public final class IndexToIndexMultiMapFactory {

    private IndexToIndexMultiMapFactory() {
        // Can't instantiate
    }

    // For test coverage
    static {
        new IndexToIndexMultiMapFactory();
    }

    public static IndexToIndexMultiMap buildIndexToIndexMultiMap(
            final int uniqueValuesCount,
            final int documentsCount) {
        if (uniqueValuesCount <= 0)
            throw new IllegalArgumentException("Nonpositive values count");
        if (documentsCount <= 0)
            throw new IllegalArgumentException("Nonpositive documents count");

        if (((long) uniqueValuesCount) * documentsCount / 64L <
            documentsCount * 4L) {
            // BitSet might be more effective
            return new BitSetIndexToIndexMultiMap(documentsCount);
        } else {
            return new IntIndexToIndexMultiMap();
        }
    }
}
