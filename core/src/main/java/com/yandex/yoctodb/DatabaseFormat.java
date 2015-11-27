/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.immutable.DatabaseReader;
import com.yandex.yoctodb.mutable.DatabaseBuilder;
import com.yandex.yoctodb.mutable.DocumentBuilder;
import com.yandex.yoctodb.v1.V1DatabaseFormat;

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
