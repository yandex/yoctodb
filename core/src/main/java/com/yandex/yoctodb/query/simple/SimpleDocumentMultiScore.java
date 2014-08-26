/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.query.simple;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.query.DocumentScore;
import com.yandex.yoctodb.query.Order;
import com.yandex.yoctodb.util.UnsignedByteArrays;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Document score based on sort fields {@link ByteBuffer}s
 *
 * @author incubos
 */
@Immutable
final class SimpleDocumentMultiScore
        implements DocumentScore<SimpleDocumentMultiScore> {
    @NotNull
    private final Order.SortOrder[] orders;
    @NotNull
    private final ByteBuffer[] values;

    public SimpleDocumentMultiScore(
            @NotNull
            final Order.SortOrder[] orders,
            @NotNull
            final ByteBuffer[] values) {
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
