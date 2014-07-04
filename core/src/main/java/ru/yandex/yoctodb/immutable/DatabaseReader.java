/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.immutable;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * Builds immutable {@link Database} from file or {@link ByteBuffer}
 *
 * @author incubos
 */
@ThreadSafe
public interface DatabaseReader {
    @NotNull
    Database from(
            @NotNull
            ByteBuffer b) throws IOException;

    @NotNull
    Database composite(
            @NotNull
            Collection<Database> databases) throws IOException;

    @NotNull
    Database from(
            @NotNull
            File f,
            boolean forceToMemory) throws IOException;
}
