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

import net.jcip.annotations.Immutable;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.query.DocumentScore;
import com.yandex.yoctodb.query.ScoredDocument;

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
