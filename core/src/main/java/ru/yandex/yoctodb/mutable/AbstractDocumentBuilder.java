/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
