/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.query.DocumentProcessor;
import com.yandex.yoctodb.query.Query;

/**
 * An immutable database
 *
 * @author incubos
 */
@Immutable
public interface Database {
    int getDocumentCount();

    @NotNull
    Buffer getDocument(int i);

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
