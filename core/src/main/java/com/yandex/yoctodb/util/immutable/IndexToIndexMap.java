/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import net.jcip.annotations.Immutable;

/**
 * @author svyatoslav
 *         Date: 21.11.13
 */
@Immutable
public interface IndexToIndexMap {
    int get(int key);
    int size();
}
