/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
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
        throw new UnsupportedOperationException("Comparing is not supported");
    }
}
