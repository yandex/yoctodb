/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Something writable to {@link OutputStream}
 *
 * @author incubos
 */
@ThreadSafe
public interface OutputStreamWritable {
    long getSizeInBytes();

    void writeTo(
            @NotNull
            OutputStream os) throws IOException;
}
