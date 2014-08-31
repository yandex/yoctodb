/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

/**
 * Select query
 *
 * @author incubos
 */
@NotThreadSafe
public interface Select extends Query {
    @NotNull
    Where where(
            @NotNull
            Condition condition);

    @NotNull
    OrderBy orderBy(
            @NotNull
            Order order);

    @NotNull
    Select skip(int skip);

    @NotNull
    Select limit(int limit);
}
