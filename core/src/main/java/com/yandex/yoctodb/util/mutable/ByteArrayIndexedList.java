/*
 * (C) YANDEX LLC, 2014-2015
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter referred to as the "License").
 *
 * A copy of the License is also available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.mutable;

import net.jcip.annotations.NotThreadSafe;
import org.jetbrains.annotations.NotNull;
import com.yandex.yoctodb.util.UnsignedByteArray;
import com.yandex.yoctodb.util.OutputStreamWritable;

/**
 * Indexed list of {@link com.yandex.yoctodb.util.UnsignedByteArray}s
 *
 * @author incubos
 */
@NotThreadSafe
public interface ByteArrayIndexedList extends OutputStreamWritable {
    void add(
            @NotNull
            UnsignedByteArray e);
}
