/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import com.yandex.yoctodb.util.OutputStreamWritable;

/**
 * @author svyatoslav
 */
@NotThreadSafe
public interface BitSetBasedIndexToIndexMultiMap extends OutputStreamWritable {
    void add(int key, ArrayBitSet bitSet);
}
