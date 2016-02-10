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
    byte[] payload = null;

    final TreeMultimap<String, UnsignedByteArray> fields =
            TreeMultimap.create();
    final Map<String, IndexOption> index = new HashMap<String, IndexOption>();
    final Map<String, LengthOption> length = new HashMap<String, LengthOption>();

    @NotNull
    @Override
    public DocumentBuilder withPayload(
            @NotNull
            final byte[] payload) {
        checkNotFrozen();

        if (this.payload != null) {
            throw new IllegalStateException("The payload is already set");
        }

        this.payload = payload;

        return this;
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
            final LengthOption length) {
        checkNotFrozen();

        this.fields.put(name, value);

        final IndexOption previousIndex = this.index.put(name, index);
        if (previousIndex != null && previousIndex != index)
            throw new IllegalArgumentException(
                    "Current index <" + index + "> for name <" + name +
                            "> differs from <" + previousIndex + ">");

        final LengthOption previousLength = this.length.put(name, length);
        if (previousLength != null && previousLength != length)
            throw new IllegalArgumentException(
                    "Current length <" + length + "> for name <" + name +
                            "> differs from <" + previousLength + ">");

        return this;
    }

    @Override
    public void check() {
        if (payload == null) {
            throw new IllegalStateException("The payload is not set");
        }
    }
}
