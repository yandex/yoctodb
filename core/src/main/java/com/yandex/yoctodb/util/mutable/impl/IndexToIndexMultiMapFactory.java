/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable.impl;

import com.yandex.yoctodb.util.mutable.IndexToIndexMultiMap;

/**
 * @author svyatoslav
 */
public class IndexToIndexMultiMapFactory {

    public static IndexToIndexMultiMap buildIndexToIndexMultiMap(
            final int documentsCount,
            final int uniqueValuesCount) {
        assert documentsCount > 0;
        assert uniqueValuesCount > 0;

        if (1.0 * uniqueValuesCount * documentsCount / 64.0 <
                documentsCount * 4.0) {
            // BitSet might be more effective
            return new BitSetIndexToIndexMultiMap(documentsCount);
        } else {
            return new IntIndexToIndexMultiMap();
        }
    }
}
