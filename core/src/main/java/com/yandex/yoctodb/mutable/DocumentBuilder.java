/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.mutable;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArray;

/**
 * Builds a document to be feed to {@link DatabaseBuilder}
 *
 * @author incubos
 */
@NotThreadSafe
public interface DocumentBuilder {
    enum IndexOption {
        FILTERABLE,
        FILTERABLE_TRIE_BASED,
        SORTABLE,
        FULL
    }

    enum LengthOption {
        FIXED,
        VARIABLE
    }

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
            LengthOption length);

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
            @NotNull
            String value,
            @NotNull
            IndexOption index);

    /**
     * Check invariants
     */
    void check();
}
