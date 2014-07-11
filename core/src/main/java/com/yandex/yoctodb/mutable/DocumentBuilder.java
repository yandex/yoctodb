/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
