/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
