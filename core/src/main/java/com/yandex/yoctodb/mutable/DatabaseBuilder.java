/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
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
