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

import com.yandex.yoctodb.immutable.Database;
import com.yandex.yoctodb.immutable.IndexedDatabase;
import com.yandex.yoctodb.query.BitSetPool;
import com.yandex.yoctodb.query.BitSetPoolPool;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Contains {@link Database} and {@link BitSetPool}
 *
 * @author incubos
 */
public final class V1QueryContext implements Closeable {
    @NotNull
    private final IndexedDatabase database;
    @NotNull
    private final BitSetPoolPool bitSetPoolPool;
    @NotNull
    private final BitSetPool bitSetPool;

    protected V1QueryContext(
            @NotNull
            final IndexedDatabase database,
            @NotNull
            final BitSetPoolPool bitSetPoolPool) {
        this.database = database;
        this.bitSetPoolPool = bitSetPoolPool;
        this.bitSetPool = bitSetPoolPool.borrowPool();
    }

    @NotNull
    public IndexedDatabase getDatabase() {
        return database;
    }

    public @NotNull BitSetPool getBitSetPool() {
        return bitSetPool;
    }

    @Override
    public void close() {
        bitSetPoolPool.returnPool(bitSetPool);
    }
}
