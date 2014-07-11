/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.query.simple;

import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.util.mutable.BitSet;

import java.util.Iterator;

/**
 * {@link Iterator} implementation mapping document IDs to {@link
 * IdScoredDocument}s
 *
 * @author incubos
 */
public final class IdScoredDocumentIterator
        implements Iterator<IdScoredDocument> {
    @NotNull
    private final Database database;
    @NotNull
    private final BitSet docs;
    private int currentDoc;

    public IdScoredDocumentIterator(
            @NotNull
            final Database database,
            @NotNull
            final BitSet docs) {
        this.database = database;
        this.docs = docs;
        currentDoc = docs.nextSetBit(0);
    }

    @Override
    public boolean hasNext() {
        return currentDoc >= 0;
    }

    @Override
    public IdScoredDocument next() {
        assert currentDoc >= 0;

        final int id = currentDoc;
        currentDoc = docs.nextSetBit(currentDoc + 1);

        return new IdScoredDocument(database, id);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removal is not supported");
    }
}
