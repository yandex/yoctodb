/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.mutable;

import com.google.common.collect.TreeMultimap;
import com.yandex.yoctodb.util.UnsignedByteArrays;
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.mutable.AbstractDocumentBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.util.UnsignedByteArray;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link DocumentBuilder} implementation in V1 format
 *
 * @author incubos
 */
@NotThreadSafe
public final class V1DocumentBuilder extends AbstractDocumentBuilder {
    final TreeMultimap<String, UnsignedByteArray> fields =
            TreeMultimap.create();
    final Map<String, IndexOption> index = new HashMap<>();
    final Map<String, IndexType> length = new HashMap<>();

    @NotNull
    @Override
    public DocumentBuilder withPayload(
            @NotNull
            final byte[] payload) {
        checkNotFrozen();

        return doWithField(
                PAYLOAD,
                UnsignedByteArrays.from(payload),
                IndexOption.STORED,
                IndexType.VARIABLE_LENGTH);
    }

    @NotNull
    @Override
    public DocumentBuilder withField(
            @NotNull
            final String name,
            @NotNull
            final UnsignedByteArray value,
            @NotNull
            final IndexOption index,
            @NotNull
            final IndexType length) {
        if (name.equals(PAYLOAD))
            throw new IllegalArgumentException("Reserved field name <" + PAYLOAD + ">");

        return doWithField(name, value, index, length);
    }

    @NotNull
    private DocumentBuilder doWithField(
            @NotNull
            final String name,
            @NotNull
            final UnsignedByteArray value,
            @NotNull
            final IndexOption index,
            @NotNull
            final IndexType length) {
        checkNotFrozen();

        this.fields.put(name, value);

        final IndexOption previousIndex = this.index.put(name, index);
        if (previousIndex != null && previousIndex != index)
            throw new IllegalArgumentException(
                    "Current index <" + index + "> for name <" + name +
                            "> differs from <" + previousIndex + ">");

        final IndexType previousLength = this.length.put(name, length);
        if (previousLength != null && previousLength != length)
            throw new IllegalArgumentException(
                    "Current length <" + length + "> for name <" + name +
                            "> differs from <" + previousLength + ">");

        return this;
    }
}
