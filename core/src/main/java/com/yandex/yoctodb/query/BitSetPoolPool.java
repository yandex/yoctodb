/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query;

import org.jetbrains.annotations.NotNull;

/**
 * Pool of {@link BitSetPool}s
 *
 * @author incubos
 */
public interface BitSetPoolPool {
    @NotNull
    BitSetPool borrowPool();

    void returnPool(
            @NotNull
            BitSetPool pool);
}
