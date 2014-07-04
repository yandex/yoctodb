/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.util.mutable;

import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.OutputStreamWritable;

/**
 * @author svyatoslav
 */
public interface TrieBasedByteArraySet extends OutputStreamWritable {
    /**
     * Adds element to this set
     *
     * @param e element to add
     * @return is element added to this set
     */
    UnsignedByteArray add(
            @NotNull
            UnsignedByteArray e);

    int indexOf(
            @NotNull
            UnsignedByteArray e);
}
