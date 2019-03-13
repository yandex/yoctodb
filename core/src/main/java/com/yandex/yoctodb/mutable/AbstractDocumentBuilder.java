/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.mutable;

import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import com.yandex.yoctodb.v1.mutable.segment.Freezable;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

/**
 * Implements generic methods of {@link DocumentBuilder}
 *
 * @author incubos
 */
@NotThreadSafe
public abstract class AbstractDocumentBuilder
        extends Freezable
        implements DocumentBuilder {
    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            @NotNull
            final UnsignedByteArray value,
            @NotNull
            final IndexOption index) {
        return withField(name, value, index, IndexType.VARIABLE_LENGTH);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            final boolean value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, IndexType.FIXED_LENGTH);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            final byte value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, IndexType.FIXED_LENGTH);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            final short value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, IndexType.FIXED_LENGTH);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            final int value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, IndexType.FIXED_LENGTH);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            final long value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, IndexType.FIXED_LENGTH);
    }

    public DocumentBuilder withField(
            @NotNull
            final String name,
            final char value,
            @NotNull
            final IndexOption index) {
        return withField(name, UnsignedByteArrays.from(value), index, IndexType.FIXED_LENGTH);
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
        return withField(name, UnsignedByteArrays.from(value), index, IndexType.VARIABLE_LENGTH);
    }
}
