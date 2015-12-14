/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable;

import com.yandex.yoctodb.immutable.DocumentProvider;
import com.yandex.yoctodb.immutable.IndexedDatabase;
import com.yandex.yoctodb.query.BitSetPoolPool;
import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.v1.query.ThreadLocalCachedBitSetPoolPool;
import org.jetbrains.annotations.NotNull;

/**
 * Constructs {@link V1QueryContext}s
 *
 * @author incubos
 */
public final class V1QueryContextFactory implements DocumentProvider {
    @NotNull
    private final IndexedDatabase database;
    @NotNull
    private final BitSetPoolPool bitSetPoolPool;

    public V1QueryContextFactory(
            @NotNull
            final IndexedDatabase database,
            final int bitSetsPerRequest) {
        this.database = database;
        this.bitSetPoolPool =
                new ThreadLocalCachedBitSetPoolPool(
                        database.getDocumentCount(),
                        bitSetsPerRequest);
    }

    @NotNull
    public V1QueryContext newContext() {
        return new V1QueryContext(database, bitSetPoolPool);
    }

    @Override
    public int getDocumentCount() {
        return database.getDocumentCount();
    }

    @NotNull
    @Override
    public Buffer getDocument(final int i) {
        return database.getDocument(i);
    }

    @NotNull
    @Override
    public Buffer getFieldValue(
            final int document,
            @NotNull
            final String fieldName) {
        return database.getFieldValue(document, fieldName);
    }
}
