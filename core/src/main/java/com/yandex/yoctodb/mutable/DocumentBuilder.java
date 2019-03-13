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
import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;

/**
 * Builds a document to be feed to {@link DatabaseBuilder}
 *
 * @author incubos
 */
@NotThreadSafe
public interface DocumentBuilder {
    enum IndexOption {
        FILTERABLE,
        SORTABLE,
        FULL,
        STORED,
        // For unit tests
        UNSUPPORTED
    }

    enum IndexType {
        FIXED_LENGTH,
        VARIABLE_LENGTH,
        TRIE
    }

    @Deprecated
    String PAYLOAD = "_payload";

    /**
     * Add payload
     *
     * @deprecated Use stored fields
     * @see IndexOption#STORED
     * @param payload payload
     * @return this
     */
    @Deprecated
    @NotNull
    DocumentBuilder withPayload(
            @NotNull
            byte[] payload);

    @NotNull
    DocumentBuilder withField(
            @NotNull
            String name,
            @NotNull
            UnsignedByteArray value,
            @NotNull
            IndexOption index,
            @NotNull
            IndexType length);

    @NotNull
    DocumentBuilder withField(
            @NotNull
            String name,
            @NotNull
            UnsignedByteArray value,
            @NotNull
            IndexOption index);

    @NotNull
    DocumentBuilder withField(
            @NotNull
            String name,
            boolean value,
            @NotNull
            IndexOption index);

    @NotNull
    DocumentBuilder withField(
            @NotNull
            String name,
            byte value,
            @NotNull
            IndexOption index);

    @NotNull
    DocumentBuilder withField(
            @NotNull
            String name,
            short value,
            @NotNull
            IndexOption index);

    @NotNull
    DocumentBuilder withField(
            @NotNull
            String name,
            int value,
            @NotNull
            IndexOption index);

    @NotNull
    DocumentBuilder withField(
            @NotNull
            String name,
            long value,
            @NotNull
            IndexOption index);

    @NotNull
    DocumentBuilder withField(
            @NotNull
            String name,
            char c,
            @NotNull
            IndexOption index
    );

    @NotNull
    DocumentBuilder withField(
            @NotNull
            String name,
            @NotNull
            String value,
            @NotNull
            IndexOption index);
}
