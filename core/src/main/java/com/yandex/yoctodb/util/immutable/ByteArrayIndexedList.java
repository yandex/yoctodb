/*
 * (C) YANDEX LLC, 2014-2016
 *
 * The Source Code called "YoctoDB" available at
 * https://github.com/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import com.yandex.yoctodb.util.buf.Buffer;
import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

/**
 * Indexed list of {@link com.yandex.yoctodb.util.UnsignedByteArray}s
 *
 * @author incubos
 */
@ThreadSafe
public interface ByteArrayIndexedList {
    @NotNull
    Buffer get(int i);

    int size();
}
