/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.Database;

/**
 * Scored document comparable by {@link DocumentScore}
 *
 * @author incubos
 */
@Immutable
public interface ScoredDocument<T extends ScoredDocument<T>>
        extends Comparable<T> {
    @NotNull
    Database getDatabase();

    int getDocument();
}
