/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;

/**
 * List of {@code int} values with {@code int} key
 *
 * @author incubos
 */
@Immutable
public final class IntToIntArray {
    private final int key;
    @NotNull
    private final int[] values;
    private final int count;

    public IntToIntArray(
            final int key,
            @NotNull
            final int[] values,
            final int count) {
        assert key >= 0;
        assert count > 0;
        assert values.length >= count;

        this.key = key;
        this.values = values;
        this.count = count;
    }

    public int getKey() {
        return key;
    }

    /**
     * Only first {@link #getCount()} elements have non {@code null} values
     *
     * @return values
     */
    @NotNull
    public int[] getValues() {
        return values;
    }

    public int getCount() {
        return count;
    }
}
