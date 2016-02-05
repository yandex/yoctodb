/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.query.DocumentScore;
import com.yandex.yoctodb.query.Order;
import com.yandex.yoctodb.util.UnsignedByteArrays;

import java.util.Arrays;

/**
 * Document score based on sort fields {@link Buffer}s
 *
 * @author incubos
 */
@Immutable
final class SimpleDocumentMultiScore
        implements DocumentScore<SimpleDocumentMultiScore> {
    @NotNull
    private final Order.SortOrder[] orders;
    @NotNull
    private final Buffer[] values;

    public SimpleDocumentMultiScore(
            @NotNull
            final Order.SortOrder[] orders,
            @NotNull
            final Buffer[] values) {
        assert values.length == orders.length;

        this.orders = orders;
        this.values = values;
    }

    @Override
    public int compareTo(
            @NotNull
            final SimpleDocumentMultiScore o) {
        assert Arrays.equals(orders, o.orders);
        assert values.length == o.values.length;

        for (int i = 0; i < values.length; i++) {
            final int result =
                    UnsignedByteArrays.compare(
                            values[i],
                            o.values[i]);
            if (result != 0) {
                switch (orders[i]) {
                    case ASC:
                        return result;
                    case DESC:
                        return -result;
                }
            }
        }

        return 0;
    }
}
