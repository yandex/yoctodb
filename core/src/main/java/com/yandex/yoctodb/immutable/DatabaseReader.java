/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;

/**
 * Builds immutable {@link Database} from file or {@link Buffer}
 *
 * @author incubos
 */
@ThreadSafe
public interface DatabaseReader {
    @NotNull
    Database from(
            @NotNull
            Buffer b) throws IOException;

    @NotNull
    Database composite(
            @NotNull
            Collection<Database> databases) throws IOException;

    @NotNull
    Database mmap(
            @NotNull
            File f,
            boolean forceToMemory) throws IOException;

    @NotNull
    Database from(
            @NotNull
            RandomAccessFile f) throws IOException;
}
