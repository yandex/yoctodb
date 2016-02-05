/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable;

import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;
import org.jetbrains.annotations.NotNull;

/**
 * Provides means to query {@link Database}
 *
 * @author incubos
 */
public interface ExecutionEngine {
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
     * The same as {@link #execute(Query, DocumentProcessor)}
     * but also returns count of unlimited documents (filtered documents
     * without {@code skip} and {@code limit})
     *
     * @param query     query to be executed
     * @param processor document processor
     * @return count of unlimited documents
     */
    int executeAndUnlimitedCount(
            @NotNull
            Query query,
            @NotNull
            DocumentProcessor processor);

    /**
     * The same as {@link #execute(Query, DocumentProcessor)}, but only
     * counts the documents satisfying the query
     *
     * @param query query to be executed
     * @return count of documents satisfying the query
     */
    int count(
            @NotNull
            Query query);
}
