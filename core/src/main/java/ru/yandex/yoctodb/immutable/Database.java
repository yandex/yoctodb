/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;

import java.nio.ByteBuffer;

/**
 * An immutable database
 *
 * @author incubos
 */
@Immutable
public interface Database {
    int getDocumentCount();

    @NotNull
    ByteBuffer getDocument(int i);

    /**
     * Execute {@code query} and apply {@code processor} to the documents found
     *
     * @param query     query to be executed
     * @param processor document processor
     */
    void execute(
            @NotNull
            Query query,
            @NotNull
            DocumentProcessor processor);

    /**
     * The same as {@link #execute(com.yandex.yoctodb.query.Query, com.yandex.yoctodb.query.DocumentProcessor)}
     * but also returns count of unlimited documents (filtered documents
     * without {@code skip} and {@code limit})
     *
     * @param query     query to be executed
     * @param processor document processor
     * @return          count of unlimited documents
     */
    int executeAndUnlimitedCount(
            @NotNull
            Query query,
            @NotNull
            DocumentProcessor processor);

    int count(
            @NotNull
            Query query);
}
