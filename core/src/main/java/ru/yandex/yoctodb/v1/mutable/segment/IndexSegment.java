/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.v1.mutable.segment;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.UnsignedByteArray;
import ru.yandex.yoctodb.util.OutputStreamWritableBuilder;

import java.util.Collection;

/**
 * Mutable index segment
 *
 * @author incubos
 */
@NotThreadSafe
public interface IndexSegment extends OutputStreamWritableBuilder {
    @NotNull
    IndexSegment addDocument(
            final int documentId,
            @NotNull
            final Collection<UnsignedByteArray> values);

    void setDatabaseDocumentsCount(int documentsCount);
}
