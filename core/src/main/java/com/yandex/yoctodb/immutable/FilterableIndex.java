/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.mutable.BitSet;

/**
 * Index filtering documents.
 *
 * The contract of each comparison method is:
 * Set to one bits in {@code dest} for conforming documents and
 * leave the other bits untouched.
 *
 * Each method returns {@code boolean} result stating if any bit was set.
 *
 * @author incubos
 */
@Immutable
public interface FilterableIndex extends Index {
    boolean eq(
            @NotNull
            BitSet dest,
            @NotNull
            Buffer value);

    boolean in(
            @NotNull
            BitSet dest,
            @NotNull
            Buffer... value);

    boolean lessThan(
            @NotNull
            BitSet dest,
            @NotNull
            Buffer value,
            boolean orEquals);

    boolean greaterThan(
            @NotNull
            BitSet dest,
            @NotNull
            Buffer value,
            boolean orEquals);

    boolean between(
            @NotNull
            BitSet dest,
            @NotNull
            Buffer from,
            boolean fromInclusive,
            @NotNull
            Buffer to,
            boolean toInclusive);
}
