/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.v1.immutable;

import com.yandex.yoctodb.immutable.IndexedDatabase;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.util.mutable.BitSet;
import org.jetbrains.annotations.NotNull;

/**
 * Pair of database and its document {@link BitSet}
 *
 * @author incubos
 */
public final class QueryContext {
    @NotNull
    private final BitSet docs;
    @NotNull
    private final IndexedDatabase database;
    @NotNull
    private final ArrayBitSetPool bitSetPool;

    public QueryContext(
            @NotNull
            final BitSet docs,
            @NotNull
            final IndexedDatabase database,
            @NotNull
            final ArrayBitSetPool bitSetPool) {
        assert !docs.isEmpty();
        assert database.getDocumentCount() == docs.getSize();

        this.docs = docs;
        this.database = database;
        this.bitSetPool = bitSetPool;
    }

    @NotNull
    public BitSet getDocs() {
        return docs;
    }

    @NotNull
    public IndexedDatabase getDatabase() {
        return database;
    }

    @NotNull
    public ArrayBitSetPool getBitSetPool() {
        return bitSetPool;
    }
}
