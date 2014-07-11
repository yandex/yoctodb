/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.query.simple;

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.query.ScoredDocument;

/**
 * {@link ScoredDocument} implementation based on document ID
 *
 * @author incubos
 */
@Immutable
public final class IdScoredDocument
        implements ScoredDocument<IdScoredDocument> {
    @NotNull
    private final Database database;
    private final int id;

    public IdScoredDocument(
            @NotNull
            final Database database,
            final int id) {
        assert 0 <= id && id < database.getDocumentCount();

        this.database = database;
        this.id = id;
    }

    @NotNull
    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public int getDocument() {
        return id;
    }

    @Override
    public int compareTo(
            @NotNull
            final IdScoredDocument o) {
        return Integer.compare(id, o.id);
    }
}
