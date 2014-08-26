/*
 * (C) YANDEX LLC, 2014
 *
 * The Source Code called "YoctoDB" available at
 * https://bitbucket.org/yandex/yoctodb is subject to the terms of the
 * Mozilla Public License, v. 2.0 (hereinafter - MPL).
 *
 * A copy of the MPL is available at http://mozilla.org/MPL/2.0/.
 */

package com.yandex.yoctodb.util.immutable;

import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Indexed list of {@link com.yandex.yoctodb.util.UnsignedByteArray}s
 *
 * @author incubos
 */
@ThreadSafe
public interface ByteArrayIndexedList {
    @NotNull
    ByteBuffer get(int i);

    int size();
}
