/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package ru.yandex.yoctodb;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;
import ru.yandex.yoctodb.immutable.DatabaseReader;
import ru.yandex.yoctodb.mutable.DatabaseBuilder;
import ru.yandex.yoctodb.mutable.DocumentBuilder;
import ru.yandex.yoctodb.v1.V1DatabaseFormat;

/**
 * Provides facilities to build a database using current format
 *
 * @author incubos
 */
@ThreadSafe
public abstract class DatabaseFormat {
    public final static byte[] MAGIC = {
            (byte) 0x40,
            (byte) 0xC7,
            (byte) 0x0D,
            (byte) 0xB1
    };

    @NotNull
    private final static DatabaseFormat currentFormat =
            new V1DatabaseFormat();

    @NotNull
    public static DatabaseFormat getCurrent() {
        return currentFormat;
    }

    @NotNull
    public abstract DocumentBuilder newDocumentBuilder();

    @NotNull
    public abstract DatabaseBuilder newDatabaseBuilder();

    @NotNull
    public abstract DatabaseReader getDatabaseReader();
}
