/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.mutable.BitSet;

import java.nio.ByteBuffer;

/**
 * Index filtering documents.
 *
 * It is extremely important to check {@code boolean} result of filtering
 * methods, because if there are no documents found, then {@code dest} bit set
 * might be left unchanged.
 *
 * @author incubos
 */
@Immutable
public interface FilterableIndex extends Index {
    /**
     * Sets to one bits in {@code dest} for documents having equal {@code value}
     * and zeroes the other bits
     *
     * @param dest  result accumulator
     * @param value value to compare to
     * @return whether filter returned nonempty set
     */
    boolean eq(
            @NotNull
            BitSet dest,
            @NotNull
            ByteBuffer value);

    boolean in(
            @NotNull
            BitSet dest,
            @NotNull
            ByteBuffer... value);

    boolean lessThan(
            @NotNull
            BitSet dest,
            @NotNull
            ByteBuffer value,
            boolean orEquals);

    boolean greaterThan(
            @NotNull
            BitSet dest,
            @NotNull
            ByteBuffer value,
            boolean orEquals);

    boolean between(
            @NotNull
            BitSet dest,
            @NotNull
            ByteBuffer from,
            boolean fromInclusive,
            @NotNull
            ByteBuffer to,
            boolean toInclusive);
}
