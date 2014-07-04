/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.immutable;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.query.DocumentProcessor;
import ru.yandex.yoctodb.query.Query;

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
     * The same as {@link #execute(ru.yandex.yoctodb.query.Query, ru.yandex.yoctodb.query.DocumentProcessor)}
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
