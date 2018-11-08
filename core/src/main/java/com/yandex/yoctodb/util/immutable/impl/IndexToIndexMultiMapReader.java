/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable.impl;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.immutable.IndexToIndexMultiMap;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

/**
 * @author svyatoslav
 */
public final class IndexToIndexMultiMapReader {

    private IndexToIndexMultiMapReader() {
        //
    }

    // For test coverage
    static {
        new IndexToIndexMultiMapReader();
    }

    public static IndexToIndexMultiMap from(
            final Buffer byteBuffer) {
        final int type = byteBuffer.getInt();
        if (type == V1DatabaseFormat.MultiMapType.LIST_BASED.getCode()) {
            return IntIndexToIndexMultiMap.from(byteBuffer.slice());
        } else if (type == V1DatabaseFormat.MultiMapType.LONG_ARRAY_BIT_SET_BASED.getCode()) {
            return BitSetIndexToIndexMultiMap.from(byteBuffer.slice());
        } else if (type == V1DatabaseFormat.MultiMapType.ASCENDING_BIT_SET_BASED.getCode()) {
            return AscendingBitSetIndexToIndexMultiMap.from(byteBuffer.slice());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported IndexToIndexMultiMap type: " + type);
        }
    }
}
