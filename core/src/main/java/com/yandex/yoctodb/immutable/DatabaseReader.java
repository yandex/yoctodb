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

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collection;

/**
 * Builds immutable {@link Database} from file or {@link Buffer}
 *
 * @author incubos
 */
@ThreadSafe
public abstract class DatabaseReader {
    @NotNull
    public Database from(
            @NotNull
            final Buffer buffer) throws IOException {
        return from(buffer, true);
    }

    @NotNull
    public abstract Database from(
            @NotNull
            Buffer b,
            boolean checksum) throws IOException;

    @NotNull
    public abstract Database composite(
            @NotNull
            Collection<Database> databases) throws IOException;

    @NotNull
    public abstract Database mmap(
            @NotNull
            File f,
            boolean forceToMemory) throws IOException;

    @NotNull
    public abstract Database from(
            @NotNull
            FileChannel f) throws IOException;
}
