/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import com.yandex.yoctodb.util.mutable.ArrayBitSetPool;
import com.yandex.yoctodb.util.mutable.impl.AllocatingArrayBitSetPool;
import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Collection;

/**
 * Builds immutable {@link Database} from file or {@link Buffer}
 *
 * @author incubos
 */
@ThreadSafe
public abstract class DatabaseReader {
    @NotNull
    public IndexedDatabase from(
            @NotNull
            final Buffer buffer) throws IOException {
        return from(buffer, AllocatingArrayBitSetPool.INSTANCE, true);
    }

    @NotNull
    public abstract IndexedDatabase from(
            @NotNull
            Buffer b,
            @NotNull
            ArrayBitSetPool bitSetPool,
            boolean checksum) throws IOException;

    @NotNull
    public abstract Database composite(
            @NotNull
            Collection<? extends IndexedDatabase> databases,
            @NotNull
            ArrayBitSetPool bitSetPool);

    @NotNull
    public Database composite(
            @NotNull
            final Collection<? extends IndexedDatabase> databases) {
        return composite(
                databases,
                AllocatingArrayBitSetPool.INSTANCE);
    }
}
