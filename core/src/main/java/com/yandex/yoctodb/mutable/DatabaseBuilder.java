/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
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
