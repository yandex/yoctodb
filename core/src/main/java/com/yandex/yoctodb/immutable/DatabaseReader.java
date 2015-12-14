/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
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
    public static final int DEFAULT_QUERY_DEPTH = 4;

    @NotNull
    public Database from(
            @NotNull
            final Buffer buffer) throws IOException {
        return from(buffer, DEFAULT_QUERY_DEPTH, true);
    }

    @NotNull
    public abstract Database from(
            @NotNull
            Buffer b,
            int bitSetsPerRequest,
            boolean checksum) throws IOException;

    @NotNull
    public abstract Database composite(
            @NotNull
            Collection<Database> databases,
            int bitSetsPerRequest);

    @NotNull
    public Database composite(
            @NotNull
            Collection<Database> databases) throws IOException {
        return composite(databases, DEFAULT_QUERY_DEPTH);
    }
}
