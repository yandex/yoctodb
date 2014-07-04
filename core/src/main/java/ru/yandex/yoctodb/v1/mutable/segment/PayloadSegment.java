/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.mutable.segment;

import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.OutputStreamWritableBuilder;

/**
 * Contains document payload
 *
 * @author incubos
 */
public interface PayloadSegment extends OutputStreamWritableBuilder {
    @NotNull
    PayloadSegment addDocument(
            final int documentId,
            @NotNull
            final byte[] payload);
}
