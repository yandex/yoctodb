/*
 * Copyright © 2014 Yandex
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file or
 * http://www.wtfpl.net/ for more details.
 */

package com.yandex.yoctodb.mutable;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.OutputStreamWritableBuilder;

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
