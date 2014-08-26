/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
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
