/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.query;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.immutable.Database;

/**
 * Processor of immutable database documents
 *
 * @author incubos
 */
@NotThreadSafe
public interface DocumentProcessor {
    /**
     * Process the document payload
     *
     * @param document document id
     * @param database database to get document payload from
     * @return whether the processing should continue
     */
    boolean process(
            int document,
            @NotNull
            Database database);
}
