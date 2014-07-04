/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb.query.simple;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.immutable.Database;
import ru.yandex.yoctodb.query.DocumentScore;
import ru.yandex.yoctodb.query.ScoredDocument;

/**
 * {@link ScoredDocument} implementation for simple queries
 *
 * @author incubos
 */
@Immutable
public final class SimpleScoredDocument
        implements ScoredDocument<SimpleScoredDocument> {
    @NotNull
    private final Database database;
    @NotNull
    private final SimpleDocumentMultiScore score;
    private final int document;

    public SimpleScoredDocument(
            @NotNull
            final Database database,
            @NotNull
            final SimpleDocumentMultiScore score,
            final int document) {
        assert document >= 0;

        this.database = database;
        this.score = score;
        this.document = document;
    }

    @NotNull
    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public int getDocument() {
        return document;
    }

    @Override
    public int compareTo(final SimpleScoredDocument o) {
        return score.compareTo(o.score);
    }
}
