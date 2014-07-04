/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
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
