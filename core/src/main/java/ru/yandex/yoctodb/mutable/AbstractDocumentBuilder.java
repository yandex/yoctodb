/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.mutable;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.UnsignedByteArrays;

/**
 * Implements generic methods of {@link DocumentBuilder}
 *
 * @author incubos
 */
@NotThreadSafe
public abstract class AbstractDocumentBuilder implements DocumentBuilder {
    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            final boolean value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, LengthOption.FIXED);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            final byte value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, LengthOption.FIXED);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            final short value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, LengthOption.FIXED);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            final int value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, LengthOption.FIXED);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final
            String name,
            final long value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, LengthOption.FIXED);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            @NotNull
            final String value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, LengthOption.VARIABLE);
    }
}
