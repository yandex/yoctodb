/*
 * Copyright (c) 2014 Yandex
 */

package ru.yandex.yoctodb.mutable;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.util.OutputStreamWritableBuilder;

import java.io.OutputStream;

/**
 * Builds a database of {@link DocumentBuilder}s serializable to
 * {@link OutputStream}
 *
 * @author incubos
 */
@NotThreadSafe
public interface DatabaseBuilder extends OutputStreamWritableBuilder {
    @NotNull
    DatabaseBuilder merge(
            @NotNull
            DocumentBuilder document);
}
