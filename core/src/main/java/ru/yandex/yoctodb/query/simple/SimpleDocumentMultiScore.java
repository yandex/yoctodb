/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query.simple;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.query.DocumentScore;
import ru.yandex.yoctodb.query.Order;
import ru.yandex.yoctodb.util.UnsignedByteArrays;

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
