/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.immutable;

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
